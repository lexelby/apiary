#
# $LicenseInfo:firstyear=2010&license=mit$
#
# Copyright (c) 2010, Linden Research, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# $/LicenseInfo$
#


'''
This module contains the main Apiary code.  The BeeKeeper spawns and manages
all subprocesses.  The QueenBee reads jobs from disk and enqueues them for
workers.  WorkerBee is a class to be extended by protocol-specific classes.  It
contains the basic workings of a worker client to make it simple to implement a
new protocol.  The StatsGatherer receives, aggregates, and prints status
messages from the WorkerBees.
'''

import optparse
import os
import re
import random
import socket
import sys
import tempfile
import cPickle
import MySQLdb
import time
import warnings
from datetime import datetime
from threading import Thread
from multiprocessing import Value, Queue
from multiprocessing.queues import Empty
from collections import defaultdict
from itertools import chain

from apiary.tools.childprocess import ChildProcess
from apiary.tools.debug import debug, traced_func, traced_method
from apiary.tools.stats import Tally, Level, Series
from apiary.tools.table import format_table, ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT

verbose = False


class BeeKeeper(object):
    """Manages the hive, including QueenBee, WorkerBees, and StatsGatherer."""

    def __init__(self, options, arguments):
        self.options = options
        self.arguments = arguments

        try:
            self.protocol = options.protocols[options.protocol]
        except KeyError:
            sys.exit('invalid protocol: %s (valid protocols: %s)' %
                     (options.protocol, " ".join(options.protocols)))

    def start(self):
        """Run the load test."""

        start_time = time.time()

        job_queue = Queue()
        stats_queue = Queue()

        workers = []

        delay = self.options.stagger_workers / 1000.0
        for i in xrange(self.options.workers):
            worker = WorkerBeeProcess(self.options, self.protocol, job_queue, stats_queue)
            worker.start()
            workers.append(worker)
            time.sleep(delay)

        # TODO: consider waiting until workers are ready
        #if self.options.startup_wait:
        #    print "workers started; waiting %d seconds..." % self.options.startup_wait
        #    time.sleep(self.options.startup_wait)

        stats_gatherer = StatsGatherer(self.options, stats_queue)
        stats_gatherer.start()

        queen = QueenBee(self.options, self.arguments, job_queue, stats_queue)
        queen.start()

        # Now wait while the queen does its thing.
        try:
            queen.join()
        except KeyboardInterrupt:
            print "Interrupted, shutting down..."
            queen.terminate()

        print "Waiting for workers to complete jobs and terminate (may take up to %d seconds)..." % self.options.max_ahead

        try:
            stop = Message(Message.STOP)
            for worker in xrange(self.options.workers * self.options.threads):
                job_queue.put(stop)

            # Now wait for the workers to get the message.  This may take a few
            # minutes as the QueenBee likes to stay ahead by a bit.

            for worker in workers:
                worker.join()

            # Tell the Stats Gatherer that it's done.
            stats_queue.put(Message(Message.STOP))

            # Wait for it to finish.
            stats_gatherer.join()

            print "Completed %d jobs in %0.2f seconds." % (queen.jobs_sent.value, time.time() - start_time)
        except KeyboardInterrupt:
            print "Interrupted before shutdown process completed."


class StatsGatherer(ChildProcess):
    """Gather and present stats.

    The StatsGatherer process reads messages off of the stats queue sent by the
    workers and aggregates them.  It prints a report periodically.

    See tools.stats for a description of the kinds of statistics that are
    available.
    """

    def __init__(self, options, stats_queue):
        super(StatsGatherer, self).__init__()

        self._options = options
        self._verbose = options.verbose
        self._tallies = defaultdict(Tally)
        self._levels = defaultdict(Level)
        self._series = defaultdict(Series)
        self._last_report = time.time()
        self._worker_count = 0
        self._queue = stats_queue

    @traced_method
    def worker_status(self, message):
        if message.type == Message.STOP:
            debug('Stopping stats gatherer.')
            return True
        elif message.type == Message.STAT_TALLY:
            #print "tally", message.body
            self._tallies[message.body].add()
        elif message.type == Message.STAT_LEVEL:
            #print "level", message.body[0], message.body[1]
            self._levels[message.body[0]].add(message.body[1])
        elif message.type == Message.STAT_SERIES:
            #print "series", message.body[0], message.body[1]
            self._series[message.body[0]].add(message.body[1])
        else:
            print >> sys.stderr, "Received unknown worker status: %s" % message

    def report(self):
        self._last_report = time.time()

        timestamp = datetime.now().strftime('%F %T')

        print
        print timestamp
        print "=" * len(timestamp)

        table = []

        for name, stat in chain(self._tallies.iteritems(),
                                self._levels.iteritems(),
                                self._series.iteritems()):
            report = stat.report()

            if report:
                row = [(ALIGN_RIGHT, "%s: " % name)]
                row.extend(report)
                table.append(row)

        print format_table(table) or "",

    def run_child_process(self):
        while True:
            try:
                done = self.worker_status(self._queue.get(timeout=1))
            except Empty:
                done = False

            if done or time.time() - self._last_report > self._options.stats_interval:
                self.report()

            if done:
                break


class QueenBee(ChildProcess):
    """A QueenBee process that distributes sequences of events"""

    def __init__(self, options, arguments, job_queue, stats_queue):
        super(QueenBee, self).__init__()

        self._options = options
        self._verbose = options.verbose
        self._jobs_file = arguments[0]
        self._index_file = arguments[0] + ".index"
        self._time_scale = 1.0 / options.speedup
        self._last_warning = 0
        self._skip_counter = options.skip
        self._last_job_start_time = 0
        self._skip = options.skip
        self.jobs_sent = Value('L', 0)
        self.job_queue = job_queue
        self.stats_queue = stats_queue

        if os.path.exists(self._index_file):
            self.use_index = True
        else:
            self.use_index = False

    def run_child_process(self):
        start_time = time.time() + self._options.startup_wait

        if self.use_index:
            jobs_file = open(self._index_file, 'rb')
        else:
            jobs_file = open(self._jobs_file, 'rb')

        job_num = 0

        while True:
            try:
                if self.use_index:
                    job_id, job_start_time, job_offset = cPickle.load(jobs_file)
                else:
                    job_offset = jobs_file.tell()
                    job_id, tasks = cPickle.load(jobs_file)

                    if not tasks:
                        continue

                    job_start_time = tasks[0][0]

                job_num += 1

                if self._options.ramp_time:
                    # Adjust skip counter once per second since ramp_time has
                    # one-second resolution anyway.

                    job_start_second = int(job_start_time)
                    if job_start_second > self._last_job_start_time:
                        self._skip = max(self._options.min_skip,
                                         self._options.skip - (job_start_second / self._options.ramp_time))

                    self._last_job_start_time = job_start_second

                if self._skip:
                    if self._skip_counter == 0:
                        self._skip_counter = self._skip
                    else:
                        self._skip_counter -= 1

                    if self._skip_counter != self._options.offset:
                        continue

                # Check whether we're falling behind, and throttle sending so as
                # not to overfill the queue.

                if not self._options.asap:
                    offset = job_start_time * self._time_scale - (time.time() - start_time)

                    if offset > self._options.max_ahead:
                        time.sleep(offset - self._options.max_ahead)
                    elif offset < -10.0:
                        if time.time() - self._last_warning > 60:
                            print "WARNING: Queenbee is %0.2f seconds behind." % (-offset)
                            self._last_warning = time.time()

                message = Message(Message.JOB, (start_time, job_id, self._jobs_file, job_offset))
                self.job_queue.put(message)
            except EOFError:
                break

        self.jobs_sent.value = job_num

class WorkerBee(Thread):
    """The thread that does the actual job processing"""

    EXCHANGE = 'b.direct'

    def __init__(self, options, job_queue, stats_queue):
        super(WorkerBee, self).__init__()

        self.options = options
        self.job_queue = job_queue
        self.stats_queue = stats_queue
        self.dry_run = options.dry_run
        self.asap = options.asap
        self.verbose = options.verbose >= 1
        self.debug = options.debug
        self.time_scale = 1.0 / options.speedup

    def status(self, status, body=None):
        self.stats_queue.put(Message(status, body))

    def error(self, message):
        self.status(Message.STAT_TALLY, "ERR: <%s>" % message)

    def tally(self, name):
        self.status(Message.STAT_TALLY, name)

    def level(self, name, increment):
        self.status(Message.STAT_LEVEL, (name, increment))

    def series(self, name, value):
        self.status(Message.STAT_SERIES, (name, value))

    def process_message(self, message):
        if message.type == Message.STOP:
            return True
        elif message.type == Message.JOB:
            # Messages look like this:
            # (start_time, job_id, job_file, offset)
            start_time, job_id, job_file, offset = message.body

            with open(job_file) as f:
                f.seek(offset)

                # Jobs look like this:
                # (job_id, ((time, request), (time, request), ...))
                read_job_id, tasks = cPickle.load(f)

            if job_id != read_job_id:
                print "ERROR: worker read the wrong job: expected %s, read %s" % (job_id, read_job_id)
                return

            if self.dry_run or not tasks:
                return

            started = False
            error = False

            for timestamp, request in tasks:
                target_time = timestamp * self.time_scale + start_time
                offset = target_time - time.time()

                # TODO: warn if falling behind?

                if offset > 0:
                    #print('sleeping %0.4f seconds' % offset)
                    debug('sleeping %0.4f seconds' % offset)
                    if offset > 120 and self.verbose:
                        print "long wait of %ds for job %s" % (offset, job_id)
                    time.sleep(offset)
                #elif offset < -1:
                #    print "worker fell behind by %.5f seconds" % (-offset)

                if not started:
                    self.level("Jobs Running", "+")
                    self.start_job(job_id)
                    started = True

                #print "sending request", request
                self.level("Requests Running", "+")
                request_start_time = time.time()
                error = not self.send_request(request)
                request_end_time = time.time()
                self.level("Requests Running", "-")
                self.series("Request Duration (ms)", (request_end_time - request_start_time) * 1000)
                if error:
                    break

            self.finish_job(job_id)
            self.level("Jobs Running", "-")

            if not error:
                self.tally("Job completed successfully")

    def start_job(self, job_id):
        pass

    def send_request(self, request):
        raise NotImplementedError('protocol plugin must implement send_request()')

    def finish_request(self, request):
        raise NotImplementedError('protocol plugin must implement send_request()')

    def finish_job(self, job_id):
        pass

    def run(self):
        while True:
            done = self.process_message(self.job_queue.get())

            if done:
                break

class WorkerBeeProcess(ChildProcess):
    """Manages the set of WorkerBee threads"""

    def __init__(self, options, protocol, job_queue, stats_queue):
        super(WorkerBeeProcess, self).__init__()

        self.options = options
        self.protocol = protocol
        self.threads = []
        self.job_queue = job_queue
        self.stats_queue = stats_queue

    def run_child_process(self):
        delay = self.options.stagger_threads / 1000.0
        for i in xrange(self.options.threads):
            thread = self.protocol.WorkerBee(self.options, self.job_queue, self.stats_queue)
            thread.setDaemon(True)
            thread.start()
            self.threads.append(thread)
            time.sleep(delay)

        debug("spawned %d threads" % len(self.threads))

        for thread in self.threads:
            thread.join()

        debug('worker ended')

class Message (object):
    STOP = 3
    JOB = 8
    STAT_TALLY = 9
    STAT_LEVEL = 10
    STAT_SERIES = 11

    def __init__(self, type, body=None):
        self.type = type
        self.body = body

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Message(%s, %s)" % (self.type, repr(self.body))


def add_options(parser):
    parser.add_option('-v', '--verbose',
                        default=0, action='count',
                        help='increase output (0~2 times')
    parser.add_option('-p', '--protocol', default='mysql',
                      help='''Protocol plugin to use (default: mysql).''')
    parser.add_option('--profile', default=False, action='store_true',
                      help='Print profiling data.  This will impact performance.')
    parser.add_option('--debug', default=False, action='store_true', dest='debug',
                      help='Print debug messages.')
    parser.add_option('--asap',
                      action='store_true', default=False,
                      help='send queries as fast as possible (default: off)')
    parser.add_option('-w', '--workers', metavar='N',
                      default=100, type='int',
                      help='number of worker bee processes (default: 100)')
    parser.add_option('--stagger-workers', metavar='MSEC',
                      default=0, type='int',
                      help='number of milliseconds to wait between starting workers (default: 0)')
    parser.add_option('-t', '--threads', metavar='N',
                      default=1, type='int',
                      help='number of threads per worker process (default: 1)')
    parser.add_option('--stagger-threads', metavar='MSEC',
                      default=0, type='int',
                      help='number of milliseconds to wait between starting threadss (default: 0)')
    parser.add_option('--startup-wait', metavar='SEC',
                      default=0, type='int',
                      help='number of seconds to wait after starting all workers before enqueuing jobs (default: 0)')
    parser.add_option('--speedup', default=1.0, dest='speedup', type='float',
                      help="Time multiple used when replaying query logs.  2.0 means "
                           "that queries run twice as fast (and the entire run takes "
                           "half the time the capture ran for).")
    parser.add_option('--skip', default=0, type='int', metavar='NUM',
                      help='''Skip this many jobs before running a job.  For example,
                           a value of 31 would skip 31 jobs, run one, skip 31, etc, so
                           1 out of every 32 jobs would be run.''')
    parser.add_option('--ramp-time', default=0, type='int', metavar='SECONDS',
                      help='''After this number of seconds, decrement the --skip
                           by 1.  Continue in this way until --skip reaches
                           --skip-min.''')
    parser.add_option('--min-skip', default=0, type='int', metavar='NUM',
                      help='''Lower bound on --skip when using --ramp-time.''')
    parser.add_option('--offset', default=0, type='int', metavar='NUM',
                      help='''When skipping jobs, this chooses which ones to run.  For
                              example, with a skip of 1, you could run apiary on two
                              hosts, one with an offset of 0 and one with an offset of
                              1 to run all jobs.''')
    parser.add_option('--max-ahead', default=300, type='int', metavar='SECONDS',
                      help='''How many seconds ahead the QueenBee may get in sending
                           jobs to the queue.  Only change this if apiary consumes tpp
                           much memory''')
    parser.add_option('-n', '--dry-run', default=False, action='store_true',
                      help='''Don't actually send any requests.''')
    parser.add_option('-i', '--stats-interval', type=int, default=15, metavar='SECONDS',
                      help='''How often to report statistics, in seconds. (default: %default)''')

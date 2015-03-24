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
all subprocesses.  The QueenBee reads jobs from disk and enqueues them in
RabbitMQ.  WorkerBee is a class to be extended by protocol-specific classes.  It
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
from threading import Thread
from multiprocessing import Value

import rabbitpy

from apiary.tools.childprocess import ChildProcess
from apiary.tools.transport import Transport, ConnectionError
from apiary.tools.debug import debug, traced_func, traced_method

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

        clean(self.options)

        start_time = time.time()

        workers = []

        delay = self.options.stagger_workers / 1000.0
        for i in xrange(self.options.workers):
            worker = WorkerBeeProcess(self.options, self.protocol)
            worker.start()
            workers.append(worker)
            time.sleep(delay)

        # TODO: consider waiting until workers are ready
        #if self.options.startup_wait:
        #    print "workers started; waiting %d seconds..." % self.options.startup_wait
        #    time.sleep(self.options.startup_wait)

        stats_gatherer = StatsGatherer(self.options)
        stats_gatherer.start()

        queen = QueenBee(self.options, self.arguments)
        queen.start()

        # Now wait while the queen does its thing.
        try:
            queen.join()
        except KeyboardInterrupt:
            print "Interrupted, shutting down..."
            queen.terminate()

        print "Waiting for workers to complete jobs and terminate (may take up to %d seconds)..." % self.options.max_ahead

        try:
            # All jobs have been sent to rabbitMQ.  Now tell workers to stop.
            transport = Transport(self.options)
            transport.connect()
            transport.queue('worker-job', clean=False)

            for worker in xrange(self.options.workers * self.options.threads):
                transport.send('worker-job', cPickle.dumps(Message(Message.STOP_WORKER)))

            # Now wait for the workers to get the message.  This may take a few
            # minutes as the QueenBee likes to stay ahead by a bit.

            for worker in workers:
                worker.join()

            # Tell the Stats Gatherer that it's done.
            transport.queue('worker-status', clean=False)
            transport.send('worker-status', cPickle.dumps(Message(Message.STOP_STATS_GATHERER)))

            # Wait for it to finish.
            stats_gatherer.join()

            print "Completed %d jobs in %0.2f seconds." % (queen.jobs_sent.value, time.time() - start_time)
        except KeyboardInterrupt:
            print "Interrupted before shutdown process completed."


class StatsGatherer(ChildProcess):

    def __init__(self, options):
        super(StatsGatherer, self).__init__()

        self._options = options
        self._verbose = options.verbose
        self._tally = {}
        self._tally_time = time.time() + 15.0
        self._worker_count = 0

    def tally(self, msg):
        self._tally[msg] = self._tally.get(msg, 0) + 1
        if time.time() > self._tally_time:
            self.print_tally()

    def print_tally(self):
        keys = self._tally.keys()
        keys.sort()
        print
        print "       count - message"
        print "------------   -------------------------------------------"
        for k in keys:
            print ("%12d - %s" % (self._tally[k], k))
        sys.stdout.flush()
        self._tally_time = time.time() + 15.0

    @traced_method
    def worker_status(self, msg):
        debug("received worker status: %s" % msg.body)
        body = msg.body
        message = cPickle.loads(body)
        if message.type == Message.WORKER_NEW:
            self._worker_count += 1
            debug('new-worker: now %d workers.',
                  self._worker_count)
            return
        elif message.type == Message.WORKER_HALTED:
            self._worker_count -= 1
            debug('worker-stopped: now %d workers.',
                  self._worker_count)
            return
        elif message.type == Message.STOP_STATS_GATHERER:
            debug('Stopping stats gatherer.')
            self.print_tally()
            msg.channel.basic_cancel('worker-status')
        elif message.type == Message.JOB_STARTED:
            self.tally("Started Job")
        elif message.type == Message.JOB_COMPLETED:
            self.tally("Finished Job")
        elif message.type == Message.JOB_ERROR:
            self.tally("Job aborted due to error: " % message.body)
        elif message.type == Message.JOB_TALLY:
            # Tally a generic message.
            self.tally(message.body)
        else:
            print >> sys.stderr, "Received unknown worker status: %s" % message

    def run_child_process(self):
        t = Transport(self._options)
        t.connect()
        t.usequeue('worker-status')
        if self._verbose > 2:
            print "consuming worker-status"
        t.consume('worker-status', 'worker-status', self.worker_status)
        t.wait()
        t.close()


class QueenBee(ChildProcess):
    """A QueenBee process that distributes sequences of events"""

    def __init__(self, options, arguments):
        super(QueenBee, self).__init__()

        self._options = options
        self._verbose = options.verbose
        self._jobs_file = arguments[0]
        self._index_file = arguments[0] + ".index"
        self._time_scale = 1.0 / options.speedup
        self._last_warning = 0
        self.jobs_sent = Value('L', 0)

        if os.path.exists(self._index_file):
            self.use_index = True
        else:
            self.use_index = False

    def run_child_process(self):
        transport = Transport(self._options)
        transport.connect()
        transport.queue('worker-job', clean=True)

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
                    job_start_time = tasks[0][0]

                job_num += 1

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
                message = cPickle.dumps(message)
                transport.send('worker-job', message)
            except EOFError:
                break

        self.jobs_sent.value = job_num

class WorkerBee(Thread):
    """The thread that does the actual job processing"""

    EXCHANGE = 'b.direct'

    def __init__(self, options, channel):
        super(WorkerBee, self).__init__()

        self.options = options
        self.channel = channel
        self.dry_run = options.dry_run
        self.asap = options.asap
        self.verbose = options.verbose >= 1
        self.debug = options.debug
        self.time_scale = 1.0 / options.speedup

    def status(self, status, body=None):
        #print 'status:', status, body
        message = rabbitpy.Message(self.channel, cPickle.dumps(Message(status, body)))
        message.publish(self.exchange, 'worker-status')

    def error(self, body):
        #print 'error:', body
        self.status(Message.JOB_ERROR, body)

    def tally(self, body):
        self.status(Message.JOB_TALLY, body)

    def process_message(self, msg):
        message = cPickle.loads(msg.body)

        if message.type == Message.STOP_WORKER:
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

            self.status(Message.JOB_STARTED)

            if self.dry_run or not tasks:
                self.status(Message.JOB_COMPLETED)
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
                    self.start_job(job_id)

                #print "sending request", request
                error = not self.send_request(request)
                if error:
                    break

            self.finish_job(job_id)

            if not error:
                self.status(Message.JOB_COMPLETED)

    def init_job(self, job_id):
        pass

    def send_request(self, request):
        raise NotImplementedError('protocol plugin must implement send_request()')

    def finish_request(self, request):
        pass

    def run(self):
        self.job_queue = rabbitpy.Queue(self.channel,
                                   'worker-job',
                                   durable=False,
                                   auto_delete=False)
        self.job_queue.declare()

        self.status_queue = rabbitpy.Queue(self.channel,
                                   'worker-status',
                                   durable=False,
                                   auto_delete=False)
        self.status_queue.declare()

        self.exchange = rabbitpy.Exchange(self.channel, self.EXCHANGE)
        self.exchange.declare()

        self.job_queue.bind(self.exchange, 'worker-job')
        self.status_queue.bind(self.exchange, 'worker-status')

        for message in self.job_queue.consume_messages(prefetch=1):
            #print "got message"
            done = self.process_message(message)
            message.ack()

            if done:
                break

class WorkerBeeProcess(ChildProcess):
    """Manages the set of WorkerBee threads"""

    def __init__(self, options, protocol):
        super(WorkerBeeProcess, self).__init__()

        self.options = options
        self.protocol = protocol
        self.threads = []

    def run_child_process(self):
        url = 'amqp%s://%s:%s@%s/%s' % \
            ('s' if self.options.amqp_ssl else '',
             self.options.amqp_userid,
             self.options.amqp_password,
             self.options.amqp_host,
             self.options.amqp_vhost)

        with rabbitpy.Connection(url) as conn:
            delay = self.options.stagger_threads / 1000.0
            for i in xrange(self.options.threads):
                thread = self.protocol.WorkerBee(self.options, conn.channel())
                thread.setDaemon(True)
                thread.start()
                self.threads.append(thread)
                time.sleep(delay)

            print "spawned %d threads" % len(self.threads)

            for thread in self.threads:
                thread.join()

        print 'worker ended'

def clean(options):
    transport = Transport(options)
    transport.connect()
    transport.queue('worker-job')
    transport.queue('worker-status')
    transport.close()


class Message (object):
    WORKER_NEW = 1
    WORKER_HALTED = 2
    STOP_WORKER = 3
    STOP_STATS_GATHERER = 4
    JOB_STARTED = 5
    JOB_COMPLETED = 6
    JOB_ERROR = 7
    JOB_TALLY = 9
    JOB = 8

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
    parser.add_option('--clean',
                      action='store_true', default=False,
                      help='clean up all queues')
    parser.add_option('--speedup', default=1.0, dest='speedup', type='float',
                      help="Time multiple used when replaying query logs.  2.0 means "
                           "that queries run twice as fast (and the entire run takes "
                           "half the time the capture ran for).")
    parser.add_option('--max-ahead', default=300, type='int', metavar='SECONDS',
                      help='''How many seconds ahead the QueenBee may get in sending
                           jobs to the queue.  Only change this if RabbitMQ runs out
                           of memory.''')
    parser.add_option('-n', '--dry-run', default=False, action='store_true',
                      help='''Don't actually send any requests.''')

    # Option groups:
    g = optparse.OptionGroup(parser, 'AMQP options')
    g.add_option('--amqp-host',
                      default="localhost", metavar='HOST',
                      help='AMQP server to connect to (default: %default)')
    g.add_option('--amqp-vhost',
                      default="/apiary", metavar='PATH',
                      help='AMQP virtual host to use (default: %default)')
    g.add_option('--amqp-userid',
                      default="apiary", metavar='USER',
                      help='AMQP userid to authenticate as (default: %default)')
    g.add_option('--amqp-password',
                      default="beehonest", metavar='PW',
                      help='AMQP password to authenticate with (default: %default)')
    g.add_option('--amqp-ssl',
                      action='store_true', default=False,
                      help='Enable SSL (default: not enabled)')
    parser.add_option_group(g)

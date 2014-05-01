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
from multiprocessing import Value

import amqplib.client_0_8 as amqp

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

        start_time = time.time()

        workers = []

        for i in xrange(self.options.workers):
            worker = self.protocol.WorkerBee(self.options)
            worker.start()
            workers.append(worker)

        # TODO: consider waiting until workers are ready

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

            for worker in workers:
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
            self.tally("100 Start Job")
        elif message.type == Message.JOB_COMPLETED:
            self.tally("200 OK")
        elif message.type == Message.JOB_ERROR:
            self.tally("500 %s" % message.body)
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
        self._sequence_file = arguments[0]
        self._time_scale = 1.0 / options.speedup
        self._last_warning = 0
        self.jobs_sent = Value('L', 0)

    def run_child_process(self):
        transport = Transport(self._options)
        transport.connect()
        transport.queue('worker-job', clean=True)

        start_time = time.time()

        sequence_file = open(self._sequence_file, 'rb')

        job_num = 0

        while True:
            try:
                job = cPickle.load(sequence_file)
                job_num += 1

                # Jobs look like this:
                # (job_id, ((time, SQL), (time, SQL), ...))

                # The job is ready to shove onto the wire as is.  However,
                # let's check to make sure we're not falling behind, and
                # throttle sending so as not to overfill the queue.

                if not self._options.asap and len(job[1]) > 0:
                    base_time = job[1][0][0]
                    offset = base_time * self._time_scale - (time.time() - start_time)

                    if offset > self._options.max_ahead:
                        time.sleep(offset - self._options.max_ahead)
                    elif offset < -10.0:
                        if time.time() - self._last_warning > 60:
                            print "WARNING: Queenbee is %0.2f seconds behind." % (-offset)
                            self._last_warning = time.time()

                message = Message(Message.JOB, job)
                message = cPickle.dumps(message)
                transport.send('worker-job', message)
            except EOFError:
                break

        self.jobs_sent.value = job_num


class WorkerBee(ChildProcess):
    """A WorkerBee that processes a sequences of events"""

    def __init__(self, options):
        super(WorkerBee, self).__init__()

        self.options = options
        self.dry_run = options.dry_run
        self.asap = options.asap
        self.verbose = options.verbose >= 1
        self.debug = options.debug
        self.time_scale = 1.0 / options.speedup

    def status(self, status, body=None):
        self._transport.send('worker-status', cPickle.dumps(Message(status, body)))

    def error(self, body):
        self.status(Message.JOB_ERROR, body)

    def process_job(self, msg):
        self._process_job(msg)
        msg.channel.basic_ack(msg.delivery_tag)

        message = cPickle.loads(msg.body)

        if message.type == Message.STOP_WORKER:
            msg.channel.basic_cancel('worker-job')
        elif message.type == Message.JOB:
            # Jobs look like this:
            # (job_id, ((time, request), (time, request), ...))

            job_id, tasks = message.body

            self.status(Message.JOB_STARTED)

            if self.dry_run:
                self.status(Message.JOB_COMPLETED)
                msg.channel.basic_ack(msg.delivery_tag)
                return

            self.start_job(job_id)

            for timestamp, request in tasks:
                target_time = timestamp * self._time_scale + self._start_time
                offset = target_time - time.time()

                # TODO: warn if falling behind?

                if offset > 0:
                    debug('sleeping %0.4f seconds' % offset)
                    if offset > 120 and self._verbose:
                        print "long wait of %ds for job %s" % (offset, job_id)
                    time.sleep(offset)

                error = not self.send_request(request)
                if error:
                    break

            self.finish_job(job_id)

            if not error:
                self.status(Message.JOB_COMPLETED)

        msg.channel.basic_ack(msg.delivery_tag)

    def init_job(self, job_id):
        raise NotImplementedError('protocol plugin must implement init_job()')

    def send_request(self, request):
        raise NotImplementedError('protocol plugin must implement send_request()')

    def run_child_process(self):
        self._transport = Transport(self.options)
        self._transport.connect()
        self._transport.set_prefetch(1)
        self._transport.usequeue('worker-job')
        self._transport.usequeue('worker-status')
        self.status(Message.WORKER_NEW)

        self._transport.consume('worker-job', 'worker-job', self.process_job, exclusive=False)
        self._transport.wait()
        self.status(Message.WORKER_HALTED)
        debug("worker ended")
        self._transport.close()
        self._transport = None


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

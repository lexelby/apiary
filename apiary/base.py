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
This module stores the base classes for the QueenBee, BeeKeeper, and
WorkerBee classes.  It is responsible for all things related to message
dispatch and collection.  It contains nothing specific to the target
protocol, nor to configuration, nor process management.
'''

# *FIX: Some bug exists which leads to the same span being inserted multiple times in http.
# It probably has to do with the interplay between message semantics.

import optparse
import os
import random
import socket
import sys
import tempfile
import threading
import time

import amqplib.client_0_8 as amqp

from apiary.tools import stattools
from apiary.tools.debug import debug, traced_func, traced_method

# We use an amqp virtual host called "/apiary".
# A virtual host holds a cluster of exchanges, queues, and bindings.
# We use a virtual host for permissions purposes (user apiary has access to everything in /apiary)
# Exchanges are routers with routing tables.  
# Queues are where your messages end up.
# Bindings are rules for routing tables.  We use a "direct" exchange.

# credit: http://blogs.digitar.com/jjww/

amqp_host = 'localhost'
amqp_userid = 'apiary'
amqp_password = 'beehonest'
amqp_vhost = '/apiary'
amqp_exchange = 'b.direct'
verbose = False

class TimeoutError(Exception):
    pass

class ConnectionError(Exception):
    pass


class Transport(object):
    """A simple message queue-like transport system
    
    Built on AMQP, hides much of the details of that interface and presents
    a simple set of utilities for sending and receiving messages on named
    queues.
    
    """
        
    def __init__(self, options=None):
        self._amqp_host = getattr(options, 'amqp_host', amqp_host)
        self._amqp_vhost = getattr(options, 'amqp_vhost', amqp_vhost)
        self._amqp_userid = getattr(options, 'amqp_userid', amqp_userid)
        self._amqp_password = getattr(options, 'amqp_password', amqp_password)
        self._verbose = getattr(options, 'verbose', verbose)
    
    def _server_connect(self):
        try:
            if self._verbose >= 2:
                print "connecting to amqp '%s@%s%s'" % (self._amqp_userid, self._amqp_host, self._amqp_vhost)
            self._conn = amqp.Connection(
                    self._amqp_host, virtual_host=self._amqp_vhost,
                    userid=self._amqp_userid, password=self._amqp_password)
        except socket.error, e:
            raise ConnectionError("Error connecting to '%s': %s" % (self._amqp_host, e))

        self._ch = self._conn.channel()
        self._ch.access_request('/data', active=True, write=True, read=True)
        self._ch.exchange_declare(amqp_exchange, 'direct', durable=False, auto_delete=False)
    
    def _server_close(self):
        try:
            self._ch.close()
            self._ch = None
        except:
            pass
        try:
            self._conn.close()
            self._conn = None
        except:
            pass
       
    def connect(self):
        self._server_connect()
        self._queues = []
        
    def close(self):
        for qname in self._queues:
            self._ch.queue_delete(qname)
        self._queues = []
        self._server_close()
    
    def queue(self, queue='', inControl=True, clean=False):
        queue, _, _ = self._ch.queue_declare(queue, durable=False, auto_delete=False)
        try:
            self._ch.queue_bind(queue, amqp_exchange, queue)
        except amqp.AMQPChannelException, e:
            sys.exit("Error binding to queue: %s" % e[1])
        if inControl:
            self._queues.append(queue)
        if clean:
        # we purge the queues when we first initialize them
            if self._verbose >= 2:
                print "purging queue " + queue
            self._ch.queue_purge(queue)
        return queue

    # same as queue(), only without inControl, so it consumes instead of appending 
    def usequeue(self, queue, clean=False):
        self.queue(queue, inControl=False, clean=clean)

    @traced_method
    def send(self, queue, data):
        msg = amqp.Message(data)
        self._ch.basic_publish(msg, amqp_exchange, queue)
    
    def consume(self, queue, tag, fn):
        fn = traced_func(fn)
        return self._ch.basic_consume(
            queue,
            tag,
            no_ack=True,
            exclusive=True,
            callback=fn)

    def cancelconsume(self, tag):
        self._ch.basic_cancel(tag)
    
    def wait(self):
        while self._ch.callbacks:
            self._ch.wait()


_STATE_WAITING_PARTIAL  = 0
_STATE_WAITING_COMPLETE = 1
_STATE_RUNNING_PARTIAL  = 2
_STATE_RUNNING_COMPLETE = 3
    
class BeeKeeper(object):
    """Maintains job status"""
    
    def __init__(self, options):
        self._options = options
        self._verbose = options.verbose
        self._throttle = options.throttle
        self._lock = threading.Lock()
        self._ok_to_start = threading.Event()
        self._ok_to_start.set()
        self._all_done = threading.Event()
        self._all_done.set()
        self._jobs = { }
        self._counts = [ 0, 0, 0, 0 ]
        self._results = [ ]
        self._running_stats = stattools.StatValue()
        self._waiting_stats = stattools.StatValue()
        self._workercount = 0
        self._shutdownphase = False
    
    def _recompute(self):
        # print 'counts:', ','.join(map(str, self._counts))
        running = (self._counts[_STATE_RUNNING_PARTIAL]
                    + self._counts[_STATE_RUNNING_COMPLETE])
        waiting = (self._counts[_STATE_WAITING_PARTIAL]
                    + self._counts[_STATE_WAITING_COMPLETE])
        
        self._running_stats.sample(running)
        self._waiting_stats.sample(waiting)

        if self._counts[_STATE_RUNNING_PARTIAL] > 0:
            # if there are started jobs that aren't complete, keep going
            self._ok_to_start.set()
            self._all_done.clear()
            return                    
                    
        if not self._throttle or waiting < max(100000, (100 * (running + 1))):
            self._ok_to_start.set()
        else:
            self._ok_to_start.clear()
            
        if running == 0 and waiting == 0:
            self._all_done.set()
        else:
            self._all_done.clear()
    
    def queenbee_start(self, job):
        if self._verbose >= 2:
            print "queenbee_start", job
        self._ok_to_start.wait()
        self._lock.acquire()
        self._jobs[job] = _STATE_WAITING_PARTIAL
        self._counts[_STATE_WAITING_PARTIAL] += 1
        self._recompute()
        self._lock.release()
        return True
    
    def queenbee_end(self, job):
        if self._verbose >= 2:
            print "queenbee_end", job
        self._lock.acquire()

        # *FIX: There's a race condition: if self.worker_end was called
        # on this job before then it won't exist in _jobs.
        if job in self._jobs:
            s = self._jobs[job]
            self._counts[s] -= 1
            if s == _STATE_WAITING_PARTIAL:
                s = _STATE_WAITING_COMPLETE
            if s == _STATE_RUNNING_PARTIAL:
                s = _STATE_RUNNING_COMPLETE
            self._jobs[job] = s
            self._counts[s] += 1
        self._recompute()
        self._lock.release()
    
    def worker_start(self, job):
        if self._verbose >= 2:
            print "worker_start", job
        self._lock.acquire()
        if job in self._jobs:
            s = self._jobs[job]
            self._counts[s] -= 1
            if s == _STATE_WAITING_PARTIAL:
                s = _STATE_RUNNING_PARTIAL
            if s == _STATE_WAITING_COMPLETE:
                s = _STATE_RUNNING_COMPLETE
            self._jobs[job] = s
            self._counts[s] += 1
            self._recompute()
        else:
            print "Received worker start of unknown job:", job
        self._lock.release()
    
    def worker_end(self, msg):
        result = msg.split(',', 1)
        job = result[0]
        if self._verbose >= 2:
            print "worker_end", job
        self._lock.acquire()
        if job in self._jobs:
            s = self._jobs[job]
            self._counts[s] -= 1
            del self._jobs[job]
            self._recompute()
            self._results.append(result)
        else:
            print "Received worker end of unknown job:", job
        self._lock.release()
    
    @traced_method
    def worker_status(self, msg):
        debug("received worker status: %s" % msg.body)
        body = msg.body
        if body == Messages.WorkerNew:
            self._workercount += 1
            debug('new-worker: now %d workers.',
                  self._workercount)
            return
        elif body == Messages.WorkerHalted:
            self._workercount -= 1
            debug('worker-stopped: now %d workers.',
                  self._workercount)
            assert self._workercount >= 0
            if self._shutdownphase and self._workercount == 0:
                self.stop(msg)
            return
        else:
            parts = body.split(',', 1)
            if len(parts) != 2:
                print "Received malformed status:", body
                return
            if parts[0] == 'start':
                self.worker_start(parts[1])
            elif parts[0] == 'end':
                self.worker_end(parts[1])
            else:
                print "Received unknown status:", body
            
    def not_done(self):
        return not self._all_done.isSet()
    
    def results(self):
        self._lock.acquire()
        rs = self._results
        self._results = []
        self._lock.release()
        return rs

    def run(self):
        t = Transport(self._options)
        t.connect()
        t.usequeue('worker-status')
        try:
            if self._verbose > 2:
                print "consuming beekeeper-end"
            t.consume('beekeeper-end', 'm0', self.shutdown_phase)
        except amqp.AMQPChannelException, e:
            print "Error received when trying to consume from queue 'beekeeper-end': %s" % e[1]
            t.close()
            self._all_done.set()
            return
        if self._verbose > 2:
            print "consuming worker-status"
        t.consume('worker-status', 'm1', self.worker_status)
        t.wait()
        t.close()

    # basic_cancel() with a consumer tag to stop the consume(), or it will consume forever
    # http://hg.barryp.org/py-amqplib/raw-file/tip/docs/overview.txt
    def stop(self, msg):
        msg.channel.basic_cancel('m0')
        msg.channel.basic_cancel('m1')
        print "running concurrency:", self._running_stats.format()
        print "waiting concurrency:", self._waiting_stats.format()

    def shutdown_phase(self, msg):
        debug('received shutdown message: %s', msg.body)
        self._shutdownphase = True
        if self._workercount > 0 and msg.body != Messages.TerminateBeeKeeper:
            for w in range(self._workercount):
                debug('sending %s %d/%d.',
                      Messages.StopWorker,
                      w+1,
                      self._workercount)
                msg.channel.basic_publish(
                    amqp.Message(Messages.StopWorker),
                    amqp_exchange,
                    'worker-job')
        else:
            debug('stopping beekeeper')
            # We can stop immediately because we are not waiting for
            # any workers to shutdown:
            self.stop(msg)
        

# How we encode sequences of queries
#@traced_func
def _job_encode(job, data_list):
    escaped_items = [
        item.replace('~', '~t').replace('|', '~p')
        for item in [job] + data_list]
    return '|'.join(escaped_items)

def _job_decode(message):
    escaped_items = message.split('|')
    data_list = [
        item.replace('~p', '|').replace('~t', '~')
        for item in escaped_items]
    job = data_list.pop(0)
    return (job, data_list)


class QueenBee(object):
    """A QueenBee process that distributes sequences of events"""
    
    def __init__(self, options, arguments):
        self._options = options
        self._verbose = options.verbose
        self._transport = Transport(options)
        self._send = self._transport.send
        self._jobs = {}        
        self._logtime = time.time()
        self._lastresult = time.time()
    
    # Methods to override in subclasses

    def next(self):
        """generate the next event

        Should call one of the following:
            self.start(seq)
            self.event(seq, data)
            self.end(seq)

        return False if there are no more events, True otherwise
        """
        raise NotImplemented("next() method should be implemented in child class")

    def result(self, seq, data):
        """The result returned by the workerbee"""
        raise NotImplemented("result() method should be implemented in child class")
        
    # methods that are sent by subclasses, from next()
    
    def start(self, job):
        if job not in self._jobs:
            self._jobs[job] = []
    
    def event(self, job, data):
        if job not in self._jobs:
            self._jobs[job] = [data]
        else:
            self._jobs[job].append(data)
    
    def end(self, job):
        if job not in self._jobs:
            return;
        data_list = self._jobs[job]
        del self._jobs[job]
        message = _job_encode(job, data_list)
        self._beekeeper.queenbee_start(job)
        self._send("worker-job", message)
        self._beekeeper.queenbee_end(job)
        
    def flush_results(self):
        for r in self._beekeeper.results():
            self._lastresult = time.time()
            self.result(r[0], r[1])
            
    def log(self, msg):
        t = time.time()
        # time elapsed between each action and the next action 
        print ("(%8.4f) %s -> %s" % (t - self._logtime, t, self._logtime)), msg
        self._logtime = t
    
    def main(self):
        self._transport.connect()
        self._transport.queue('beekeeper-end', clean=True)
        self._transport.queue('worker-job', clean=True)
        

        print "Initializing BeeKeeper"
        self._beekeeper = BeeKeeper(self._options)
        beekeeper_thread = threading.Thread(target=self._beekeeper.run)
        beekeeper_thread.setDaemon(True)
        beekeeper_thread.start()
        
        
        try:
            while self.next():
                self.flush_results()
        except KeyboardInterrupt:
            print "Interrupted, shutting down..."
            
        # *FIX: This is probably related to the bug in http where the same span gets repeatedly re-inserted.
        for job in self._jobs.keys():
            self.end(job)
       
        while self._beekeeper.not_done():
            self.flush_results()
            time.sleep(1.0)

        self.flush_results()

        self._send('beekeeper-end', Messages.StopBeeKeeper)
        
        start = time.time()
        
        while beekeeper_thread.is_alive():
            time.sleep(1)
            if time.time() - start >= 60:
                debug('Some WorkerBees did not report back that they had halted; terminating beekeeper anyway.')
                self._send('beekeeper-end', Messages.TerminateBeeKeeper)
                break
        
        beekeeper_thread.join()

        # close the queues
        if self._verbose > 2:
            print "closing transport"
        self._transport.close()


# identify the worker threads in the logging
_randomized = False
def genWorkerID():
    global _randomized
    if not _randomized:
        random.jumpahead(os.getpid())
        _randomized = True
    return "worker-%02d-%02d-%02d" % (
        random.randint(0,99),
        random.randint(0,99),
        random.randint(0,99))

class WorkerBee(object):
    """A WorkerBee that processes a sequences of events"""
    
    def __init__(self, options, arguments):
        self._asap = options.asap
        self._error = False
        self._errormsg = ''
        self._id = genWorkerID()
        self._transport = Transport(options)
        self._send = self._transport.send
        self._verbose = options.verbose >= 1
        self._logtime = time.time()
        self._wait_for_more = True
    
    # Methods to override in subclasses
    
    def start(self):
        """start of a sequence of events"""
        raise NotImplemented("start() method should be implemented by child class")
    
    def event(self, data):
        """an event in a sequence"""
        raise NotImplemented("event() method should be implemented by child class")
    
    def end(self):
        """the end of a sequence"""
        return ''
    
    def log(self, msg):
        if self._verbose < 1:
            return
        t = time.time()
        # print time elapsed between each event and the next event 
        print ("%s (%8.4f) %s -> %s" % (self._id, t - self._logtime, t, self._logtime)), msg
        self._logtime = t
    

    # Implementation
    
    def main(self):
        self._transport.connect()
        self._transport.usequeue('worker-job')
        self._transport.usequeue('worker-status')
        self._send('worker-status', Messages.WorkerNew)

        consumertag = 'm0'

        def handle_message(amqpmsg):
            body = amqpmsg.body
            if body == Messages.StopWorker:
                debug('Received %s.', Messages.StopWorker)
                self._wait_for_more = False
                self._send('worker-status', Messages.WorkerHalted)
                self._transport._ch.basic_cancel(consumertag)
                return

            (job, data_list) = _job_decode(amqpmsg.body)

            self._send('worker-status', 'start,' + job)
            self.log("starting job")
            self.start()
            for item in data_list:
                self.event(item)
            result = self.end()
            self.log("ending job")
            self._send('worker-status', 'end,' + job + ',' + result)

        self._transport._ch.basic_consume('worker-job',
                                          consumer_tag=consumertag,
                                          callback=handle_message,
                                          no_ack=True,
                                          exclusive=False)

        while self._wait_for_more:
            self._transport._ch.wait()
        debug("worker ended")
        self._transport.close()
        self._transport = None


def clean(options):
    transport = Transport(options)
    transport.connect()
    transport.queue('beekeeper-end')
    transport.queue('worker-job')
    transport.queue('worker-status')
    transport.close()


class Messages (object):
    WorkerNew = 'worker-new'
    WorkerHalted = 'worker-halted'
    StopWorker = 'stop-worker'
    StopBeeKeeper = 'stop-beekeeper'
    TerminateBeeKeeper = 'terminate-beekeper'


def add_options(parser):
    parser.add_option('-v', '--verbose',
                        default=0, action='count',
                        help='increase output (0~2 times')
    parser.add_option('--profile', default=False, action='store_true', 
                      help='Print profiling data.  This will impact performance.')
    parser.add_option('--debug', default=False, action='store_true', dest='debug',
                      help='Print debug messages.')

    # Option groups:
    for name, addopts in [('AMQP', add_amqp_options),
                          ('QueenBee', add_queenbee_options),
                          ('WorkerBee', add_workerbee_options)]:
        g = optparse.OptionGroup(parser, '%s options' % name)
        addopts(g)
        parser.add_option_group(g)


def add_amqp_options(parser):
    parser.add_option('--amqp-host',
                      default=amqp_host, metavar='HOST',
                      help='AMQP server to connect to (default: %default)')
    parser.add_option('--amqp-vhost',
                      default=amqp_vhost, metavar='PATH',
                      help='AMQP virtual host to use (default: %default)')
    parser.add_option('--amqp-userid',
                      default=amqp_userid, metavar='USER',
                      help='AMQP userid to authenticate as (default: %default)')
    parser.add_option('--amqp-password',
                      default=amqp_password, metavar='PW',
                      help='AMQP password to authenticate with (default: %default)')
    parser.add_option('--amqp-ssl',
                      action='store_true', default=False,
                      help='Enable SSL (default: not enabled)')
    return parser


def add_queenbee_options(parser):
    parser.add_option('-c', '--queenbee',
                      default=False, action='store_true',
                      help='run a queenbee (queenbee job distributor)')
    parser.add_option('--throttle',
                      default=False, action='store_true',
                      help='attempt to throttle jobs in queue')
    return parser
    

def add_workerbee_options(parser):
    parser.add_option('--asap',
                      action='store_true', default=False,
                      help='send queries as fast as possible (default: off)')
    parser.add_option('-w', '-f', '--workers', metavar='N',
                      default=0, type='int',
                      help='fork N workerbee processes (default: 0)')
    parser.add_option('-b', '--background', default=False,
                      action='store_true', help="Detach after forking workers.")
    parser.add_option('--clean',
                      action='store_true', default=False,
                      help='clean up all queues, causing old workers to quit')
    return parser

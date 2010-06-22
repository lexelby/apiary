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

from optparse import OptionParser
import os
import random
import sys
import thread
import threading
import time

import amqplib.client_0_8 as amqp

import stattools


# We use an amqp virtual host called "/hive".
# A virtual host holds a cluster of exchanges, queues, and bindings.
# We use a virtual host for permissions purposes (user hive has access to everything in /hive)
# Exchanges are routers with routing tables.  
# Queues are where your messages end up.
# Bindings are rules for routing tables.  We use a "direct" exchange.

# credit: http://blogs.digitar.com/jjww/

amqp_host = 'localhost'
amqp_userid = 'hive'
amqp_password = 'resistanceisfutile'
amqp_vhost = '/hive'
amqp_exchange = 'b.direct'
timeout = 10.0

class TimeoutError(Exception):
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
        self._timeout = getattr(options, 'timeout', timeout)
    
    def _server_connect(self):
        self._conn = amqp.Connection(
                self._amqp_host, virtual_host=self._amqp_vhost,
                userid=self._amqp_userid, password=self._amqp_password)
        self._ch = self._conn.channel()
        # request active access so we can get, create, delete
        self._ch.access_request('/data', active=True, write=True, read=True)
        # durable=False means flush messages out to disk
        # auto_delete=True means get rid of the queue when there are no more messages to consume.
        # Doesn't hardly matter though since we manually delete the queues with close
        self._ch.exchange_declare(amqp_exchange, 'direct', durable=False, auto_delete=True)
    
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
       
    # vestigial
    def _server_reconnect(self):
        self._server_close()
        self._server_connect()

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
        self._ch.queue_bind(queue, amqp_exchange, queue)
        if inControl:
            self._queues.append(queue)
        if clean:
        # we purge the queues when we first initialize them
            #print "purging queue " + queue
            self._ch.queue_purge(queue)
        return queue

    # same as queue, only without inControl, so it purges, instead of appending (these names suck)
    # we are having problems with usequeue, not with queue
    def usequeue(self, queue, clean=False):
        self.queue(queue, inControl=False, clean=clean)

    # we aren't using this -- should we be?
#    def donequeue(self, queue):
#        self._ch.queue_delete(queue)
        
    def send(self, queue, data):
        msg = amqp.Message(data)
        self._ch.basic_publish(msg, amqp_exchange, queue)
    
    def recv(self, queue):
        t = time.time()
        while True:
            # m is a sequence of SQL statements (preprocessed)
            m = self._ch.basic_get(queue, no_ack=True)
            if m is not None:
                return m.body
# Should just take out TimeoutError, and return None when there's no more messages.
# Or fix TimeoutError so it closes the queues?
# Anytime we call TimeoutError, it's going to screw up the workers.
#            if (time.time() - t) > self._timeout:
#                raise TimeoutError('Timeout waiting for data on queue ' + queue)    
            if (time.time() - t) > self._timeout:
                #print 'Timeout waiting for data on queue ' + queue 
                break

            time.sleep(0.1)

    def consume(self, queue, tag, fn):
        return self._ch.basic_consume(queue, tag,
                            no_ack=True, exclusive=True, callback=fn)

    def cancelconsume(self, tag):
        self._ch.basic_cancel(tag)
    
    def wait(self):
        while self._ch.callbacks:
            self._ch.wait()


_STATE_WAITING_PARTIAL  = 0
_STATE_WAITING_COMPLETE = 1
_STATE_RUNNING_PARTIAL  = 2
_STATE_RUNNING_COMPLETE = 3
    
class JobMinder(object):
    """A check on jobs to run"""
    
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
    
    def _recompute(self):
        #print 'counts:', ','.join(map(str, self._counts))
        # must have lock!
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
                    
        if not self._throttle or waiting < (100 * (running + 1)):
            self._ok_to_start.set()
        else:
            self._ok_to_start.clear()
            
        if running == 0 and waiting == 0:
            self._all_done.set()
        else:
            self._all_done.clear()
    
    
    def central_start(self, job):
        if self._verbose >= 2:
            print "central_start", job
        self._ok_to_start.wait()
        self._lock.acquire()
        self._jobs[job] = _STATE_WAITING_PARTIAL
        self._counts[_STATE_WAITING_PARTIAL] += 1
        self._recompute()
        self._lock.release()
        return True
    
    def central_end(self, job):
        if self._verbose >= 2:
            print "central_end", job
        self._lock.acquire()
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
#        else:
#            print "Received worker start of unknown job:", job
        self._lock.release()
    
    def worker_end(self, msg):
        result = msg.split(',', 1)
        job = result[0]
        if self._verbose >= 2:
            print "worker_end", job
        self._lock.acquire()
        if job in self._jobs:
            del self._jobs[job]
            self._counts[_STATE_RUNNING_COMPLETE] -= 1
            self._recompute()
            self._results.append(result)
#        else:
#            print "Received worker end of unknown job:", job
        self._lock.release()
    
    def worker_status(self, msg):
        parts = msg.body.split(',', 1)
        if len(parts) != 2:
            print "Received malformed status:", msg.body
            return
        if parts[0] == 'start':
            self.worker_start(parts[1])
        elif parts[0] == 'end':
            self.worker_end(parts[1])
        else:
            print "Received unknown status:", msg.body
            
    def wait_for_done(self, timeout=None):
        self._all_done.wait(timeout)
        if self._verbose >= 2:
            print
        return self._all_done.isSet()
    
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
        t.usequeue('minder-end')
        t.usequeue('worker-status')
        # so ... this never ends.  we never get to the rest of this.
        #print "consuming minder-end"
        t.consume('minder-end', 'm0', self.stop)
        #print "consuming worker-status"
        t.consume('worker-status', 'm1', self.worker_status)
        t.wait()
        t.close()

# basic_cancel() with a consumer tag to stop the consume(), or it will consume forever
# we should be using t.cancelconsume() here
# http://hg.barryp.org/py-amqplib/raw-file/tip/docs/overview.txt
    def stop(self, msg):
        msg.channel.basic_cancel('m0')
        msg.channel.basic_cancel('m1')
        print "running concurrency:", self._running_stats.format()
        print "waiting concurrency:", self._waiting_stats.format()

# How we encode sequences of queries (?)
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


class Central(object):
    """A Central process that distributes sequences of events"""
    
    def __init__(self, options, arguments):
        self._options = options
        self._timeout = options.timeout
        self._transport = Transport(options)
        self._send = self._transport.send
        self._recv = self._transport.recv
        self._jobs = {}        
    
    # Methods to override in subclasses
    
    def next(self):
        """generate the next event
        
        Should call one of the following:
            self.start(seq)
            self.event(seq, data)
            self.end(seq)
        
        return False if there are no more events, True otherwise
        """
        # what does this do?
        self.endrun()
    
    def result(self, seq, data):
        """The result returned by the worker"""
        pass
        
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
        self._minder.central_start(job)
        self._send("worker-job", message)
        self._minder.central_end(job)
        
    def flush_results(self):
        for r in self._minder.results():
            self.result(r[0], r[1])
            
    def main(self):
        self._transport.connect()
        self._transport.queue('minder-end', clean=True)
        self._transport.queue('worker-job', clean=True)
        self._transport.queue('worker-status', clean=True)

        self._minder = JobMinder(self._options)
        minder_thread = threading.Thread(target=self._minder.run)
        minder_thread.setDaemon(True)
        minder_thread.start()
        
        while self.next():
            self.flush_results()
        for job in self._jobs.keys():
            self.end(job)
       
        # Main main main loop
        while self._minder.not_done():
            self.flush_results()
            time.sleep(1.0)
        self.flush_results()
           
        self._send('minder-end', '')
        minder_thread.join(self._timeout)
        if minder_thread.isAlive():
            raise TimeoutError('Timeout waiting for job minder to exit.')

        # delete the queues -- this never happens
        print "closing transport"
        self._transport.close()


# identify the worker threads
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

class Worker(object):
    """A Worker that processes a sequences of events"""
    
    def __init__(self, options, arguments):
        self._id = genWorkerID()
        self._transport = Transport(options)
        self._send = self._transport.send
        self._recv = self._transport.recv
        self._verbose = options.verbose >= 1
        self._logtime = time.time()
    
    # Methods to override in subclasses
    
    def start(self):
        """start of a sequence of events"""
        pass
    
    def event(self, data):
        """an event in a sequence"""
        pass
    
    def end(self):
        """the end of a sequence"""
        return ''
    
    def log(self, msg):
        if self._verbose < 1:
            return
        t = time.time()
        # time elapsed between each action and the next action 
        print ("%s (%8.4f)" % (self._id, t - self._logtime)), msg
        self._logtime = t
    

    # Implementation
    
    def main(self):
        self.log("starting AMQP connection")
        self._transport.connect()
        self._transport.usequeue('worker-job')
        # should be queue, not usequeue? -- sending, not receiving?
        # what's the difference between using basic_consume and basic_get?
        self._transport.usequeue('worker-status')
        #self._transport.queue('worker-status', inControl=True)
        
        while True:
            try:
                self.log("getting from worker-job queue")
                message = self._recv('worker-job')
            except amqp.AMQPException, ae:
                self.log ("Got AMQP error: " + str(ae))
                # the break is what pops the worker out of this loop and allows it to close the AMQP connections
                # the AMQP 404 error is what causes the worker to break
                # so how do we break without a 404 error, or how do we manage to always get the error?
                # see recv
                break

            # get rid of stack traces complaining about decoding empty message
            if message is not None:
                (job, data_list) = _job_decode(message)

                self._send('worker-status', 'start,' + job)
                self.start()
                for item in data_list:
                    self.event(item)
                result = self.end()
                self._send('worker-status', 'end,' + job + ',' + result)

            # ok, I see what's happening here.  If we don't explicitly kill off the workers
            # here, it will just keep timing out and saying "timeout getting from worker-job
            # queue" every 20 seconds forever and ever.
            # So we will kill it off when we get an empty message.  But this will kill it off
            # when SOME of the workers still have things to do.  Is there a way to kill it off
            # when the last worker is empty?  Can we kill it off when the minder thinks we're
            # finished?  Or do we need a fanout queue?

            else:
#                self.log("killing AMQP connection")
                self._transport.close()
                self._transport = None
                # break, or else we will keep trying to recv from worker-job queue
                break


def clean(options):
    transport = Transport(options)
    transport.connect()
    transport.queue('minder-end')
    transport.queue('worker-job')
    transport.queue('worker-status')
    transport.close()



def start_forks(options):
    if options.workers == 0:
        options.workers = 1
    if os.fork() == 0:
        # now in child
        os.setsid() # magic that creates a new process group
        options.central = False # ensure forks don't run central
        for i in xrange(0, options.fork):
            if os.fork() == 0:
                # now in grandchild
                return # escape loop, keep processing
        sys.exit(0)
    else:
        options.workers = 0 # ensure parent doesn't run workers

def run_worker(worker_cls, options, arguments):
    w = worker_cls(options, arguments)
    try:
        w.main()
    except KeyboardInterrupt:
        thread.interrupt_main()
        
def start_workers(worker_cls, n, options, arguments):
    threads = []
    for i in xrange(0, n):
        t = threading.Thread(target=run_worker,
                                args=(worker_cls, options, arguments))
        threads.append(t)
        t.start()
    return threads

def run_central(central_cls, options, arguments):
    c = central_cls(options, arguments)
    c.main()


class Hive(object):
    def __init__(self, central_cls, worker_cls):
        self.central_cls = central_cls
        self.worker_cls = worker_cls
    
    def add_options(self, parser):
        # AMQP options
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
        
        # Central options
        parser.add_option('-c', '--central',
                            default=False, action='store_true',
                            help='run a central job distributor')
        
        parser.add_option('--throttle',
                            default=False, action='store_true',
                            help='attempt to throttle jobs in queue')
        
        # Worker options
        parser.add_option('-w', '--workers', metavar='N',
                            default=0, type='int',
                            help='create N worker threads (default: 0)')
        parser.add_option('-f', '--fork', metavar='K',
                            default=0, type='int',
                            help='fork K detached processes (default: 0)')
    
        parser.add_option('--clean',
                            action='store_true', default=False,
                            help='clean up all queues, causing old workers to quit')
        
        # Generic options
        parser.add_option('--timeout', metavar='T',
                            default=timeout, type='float',
                            help='set timeout to T seconds (default: %default)')
        parser.add_option('-v', '--verbose',
                            default=0, action='count',
                            help='increase output (0~2 times')
        

    def default_options(self):
        parser = OptionParser()
        self.add_options(parser)    
        options, arguments = parser.parse_args(args=[])
        return options
        
    def main(self, args=None):
        parser = OptionParser()
        self.add_options(parser)    
                            
        options, arguments = parser.parse_args(args=args)
        
        if options.clean:
            clean(options)
            
        if (not options.clean and not options.central
            and options.fork == 0 and options.workers == 0):
            sys.exit('Nothing to do: specify one or more of --central, --workers or --clean')
    
        if options.fork > 0:
            start_forks(options)
            
        if options.workers > 0:
            start_workers(self.worker_cls, options.workers, options, arguments)
        
        if options.central:
            run_central(self.central_cls, options, arguments)
        

if __name__ == '__main__':
    Hive(None, None).main()

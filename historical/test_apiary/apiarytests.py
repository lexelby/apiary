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

import sys
import time
import threading
import unittest

import apiary
from apiary.base import Messages
#import apiary.tools.debug ; apiary.tools.debug.enable_debug()

import amqplib.client_0_8 as amqp


class SimpleWorkerBee(apiary.WorkerBee):
    def __init__(self, options, arguments):
        apiary.WorkerBee.__init__(self, options, arguments)
        self._result = ''
    
    def start(self):
        self._result = '('
    
    def event(self, data):
        self._result += data
    
    def end(self):
        self._result += ')'
        return self._result
        

class SimpleQueenBee(apiary.QueenBee):
    def __init__(self, options, arguments):
        apiary.QueenBee.__init__(self, options, arguments)
        self.results = []
        self._events = []
        self._startfn = self.start
        self._eventfn = self.event
        self._endfn = self.end
    
    def addstart(self, seq):
        self._events.append([self._startfn, seq])
    
    def addevent(self, seq, data):
        self._events.append([self._eventfn, seq, data])
    
    def addend(self, seq):
        self._events.append([self._endfn, seq])
    
    def addstringseq(self, seq, data):
        self.addstart(seq)
        for c in data:
            self.addevent(seq, c)
        self.addend(seq)
    
    def addsleep(self, t=0.25):
        self._events.append([time.sleep, t])
        
    def next(self):
        if not self._events:
            return False
        e = self._events.pop(0)
        e[0](*e[1:])
        return True
    
    def result(self, seq, data):
        self.results.append(data)


class apiaryTestCase(unittest.TestCase):
    def setUp(self):
        args = ['--protocol', 'mysql']
        self.options = apiary.parse_args(args)[0]
        self._transport = None
        self._open = False
        self._threads = []
        apiary.clean(self.options)
        
    def tearDown(self):
        self.close()
        self.join_workers()
        # if connect() fails, don't try to clean up
        try:
            self.connect()
        except apiary.ConnectionError:
            pass
        else:
            apiary.clean(self.options)
        
    def connect(self):
        if self._transport is None:
            self._transport = apiary.Transport(self.options)
            self.send = self._transport.send
        self._transport.connect()
        self._open = True
                
    def close(self):
        if self._open:
            self._transport.close()
            self._open = False
    
    def start_workers(self, cls=SimpleWorkerBee, n=1):
        threads = [threading.Thread(target=apiary.run_worker, args=(cls, self.options, [])) for i in range(n)]
        [t.start() for t in threads]
        f = sys._getframe(1)
        name = "Worker-%s:%d-" % (f.f_code.co_name, f.f_lineno)
        for i in xrange(0,len(threads)):
            threads[i].setName(name + str(i+1))
        self._threads += threads
        
    def join_workers(self):
        for t in self._threads:
            t.join(5.0)
            if t.isAlive():
                raise Exception("thread %s never died" % t.getName())
        self._threads = []


class TestWorkerBee(apiaryTestCase):
    def testNoWork(self):
        self.start_workers(SimpleWorkerBee, 1)
        
        time.sleep(0.1)
        self.connect()
        self._transport.queue('worker-job')
        self._transport.send('worker-job', Messages.StopWorker)

        self.join_workers()
        self.close()
        

class TestBasics(apiaryTestCase):

    def testEmptyQueenBee(self):
        c = SimpleQueenBee(self.options, [])
        c.main()
        
    def testOneSeq(self):
        c = SimpleQueenBee(self.options, [])
        c.addstringseq('aaa', ['1234.5	abc'])

        self.start_workers(SimpleWorkerBee, 1)

        c.main()
        
        r = ','.join(c.results)
        self.assert_('(1234.5	abc)' in r)
        
        self.join_workers()

    def testTwoSeq(self):
        c = SimpleQueenBee(self.options, [])
        c.addstringseq('aaa', ['1234.5	abc'])
        c.addstringseq('xxx', ['4321.0	xyz'])
        
        self.start_workers(SimpleWorkerBee, 1)
        
        c.main()
        r = ','.join(c.results)
        self.assert_('(1234.5	abc)' in r)
        self.assert_('(4321.0	xyz)' in r)
        
        self.join_workers()

        
    def testTwoSeq2(self):
        c = SimpleQueenBee(self.options, [])
        c.addstringseq('aaa', ['1234.5	abc'])
        c.addstringseq('xxx', ['4321.0	xyz'])
        
        self.start_workers(SimpleWorkerBee, 2)
        
        c.main()
        r = ','.join(c.results)
        self.assert_('(1234.5	abc)' in r)
        self.assert_('(4321.0	xyz)' in r)

        self.join_workers()

    def testTwoSeq2Interleaved(self):
        c = SimpleQueenBee(self.options, [])
        c.addstart('aaa')
        c.addstart('xxx')
        c.addsleep()
        c.addevent('aaa', '1234.5	a')
        c.addevent('xxx', '4321.0	x')
        c.addsleep()
        c.addevent('xxx', '4321.1	y')
        c.addevent('aaa', '1234.6	b')
        c.addsleep()
        c.addevent('aaa', '1234.7	c')
        c.addevent('xxx', '4321.2	z')
        c.addsleep()
        c.addend('aaa')
        c.addend('xxx')
        
        self.start_workers(SimpleWorkerBee, 2)
        
        c.main()
        r = ','.join(c.results)
        self.assert_('(1234.5	a1234.6	b1234.7	c)' in r)
        self.assert_('(4321.0	x4321.1	y4321.2	z)' in r)
        
        self.join_workers()


class TestErrors(apiaryTestCase):
    def do_nothing(self):
        pass

    def testConnectionError(self):
        self.options.amqp_host = "localhost:1"
        self._transport = apiary.Transport(self.options)
        self.assertRaises(apiary.ConnectionError, self.connect)
 
    def testExclusiveQueueUse(self):
        self._transport = apiary.Transport(self.options)
        self._transport.connect()
        self._transport.usequeue('minder-end')
        self._transport.consume('minder-end', 'm0', self.do_nothing)
        self._transport2 = apiary.Transport(self.options)
        self._transport2.connect()
        self._transport2.usequeue('minder-end')
        self.assertRaises(amqp.AMQPChannelException, self._transport2.consume, 'minder-end', 'm0', self.do_nothing)
        self._transport.close()
        self._transport2.close()


if __name__ == '__main__':
    unittest.main()

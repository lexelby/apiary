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
import thread
import unittest

import hive


def create_worker(cls):
    thread.start_new_thread(hive.run_worker, (cls,))


class SimpleWorker(hive.Worker):
    def __init__(self, options, arguments):
        hive.Worker.__init__(self, options, arguments)
        self._result = ''
    
    def start(self):
        self._result = '('
    
    def event(self, data):
        self._result += data
    
    def end(self):
        self._result += ')'
        return self._result
        
    def main(self):
        #print "worker up..."
        hive.Worker.main(self)
        #print "worker down..."


class SimpleCentral(hive.Central):
    def __init__(self, options, arguments):
        hive.Central.__init__(self, options, arguments)
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


class HiveTestCase(unittest.TestCase):
    def setUp(self):
        self.options = hive.Hive(None, None).default_options()
        self._transport = None
        self._open = False
        self._threads = []
        hive.clean(self.options)
        
    def tearDown(self):
        self.close()
        self.join_workers()
        hive.clean(self.options)
        
    def connect(self):
        if self._transport is None:
            self._transport = hive.Transport(self.options)
            self.send = self._transport.send
            self.recv = self._transport.recv
        self._transport.connect()
        self._open = True
                
    def close(self):
        if self._open:
            self._transport.close()
            self._open = False
    
    def start_workers(self, cls=SimpleWorker, n=1):
        threads = hive.start_workers(cls, n, self.options, [])
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


class TestWorker(HiveTestCase):
    def testAMQP(self):
        self.connect()
        qname = self._transport.queue()
        self.send(qname, 'Hello')
        d = self.recv(qname)
        self.assertEqual(d, 'Hello')
        self.close()

    def testNoWork(self):
        self.start_workers(SimpleWorker, 1)
        
        time.sleep(0.1)
        self.connect()
        self._transport.queue('worker-job')
        self.close()

        self.join_workers()
        

class TestBasics(HiveTestCase):

    def testEmptyCentral(self):
        c = SimpleCentral(self.options, [])
        c.main()
        
    def testOneSeq(self):
        c = SimpleCentral(self.options, [])
        c.addstringseq('aaa', 'abc')

        self.start_workers(SimpleWorker, 1)

        c.main()
        
        r = ','.join(c.results)
        self.assert_('(abc)' in r)
        
        self.join_workers()

    def testTwoSeq(self):
        c = SimpleCentral(self.options, [])
        c.addstringseq('aaa', 'abc')
        c.addstringseq('xxx', 'xyz')
        
        self.start_workers(SimpleWorker, 1)
        
        c.main()
        r = ','.join(c.results)
        self.assert_('(abc)' in r)
        self.assert_('(xyz)' in r)
        
        self.join_workers()

        
    def testTwoSeq2(self):
        c = SimpleCentral(self.options, [])
        c.addstringseq('aaa', 'abc')
        c.addstringseq('xxx', 'xyz')
        
        self.start_workers(SimpleWorker, 2)
        
        c.main()
        r = ','.join(c.results)
        self.assert_('(abc)' in r)
        self.assert_('(xyz)' in r)

        self.join_workers()

    def testTwoSeq2Interleaved(self):
        c = SimpleCentral(self.options, [])
        c.addstart('aaa')
        c.addstart('xxx')
        c.addsleep()
        c.addevent('aaa', 'a')
        c.addevent('xxx', 'x')
        c.addsleep()
        c.addevent('xxx', 'y')
        c.addevent('aaa', 'b')
        c.addsleep()
        c.addevent('aaa', 'c')
        c.addevent('xxx', 'z')
        c.addsleep()
        c.addend('aaa')
        c.addend('xxx')
        
        self.start_workers(SimpleWorker, 2)
        
        c.main()
        r = ','.join(c.results)
        self.assert_('(abc)' in r)
        self.assert_('(xyz)' in r)
        
        self.join_workers()




class TestTimeouts(HiveTestCase):
    def setUp(self):
        HiveTestCase.setUp(self)
        self.options.timeout = 0.5

    def testWorkerNoJobsTimeout(self):
        # Worker w/no jobs stops after timeout
        w = SimpleWorker(self.options, [])
        t = time.time()
        self.assertRaises(hive.TimeoutError, w.main)
        t = time.time() - t
        self.assert_(t < 1.0, "%f < 1.0" % t)
    
    def testWorkerNoEndTimeout(self):
        # Worker w/job, but no job end, stops after timeout
        self.connect()
        qname = self._transport.queue()
        self.send(qname, 'Hello')
        self.send(qname, 'Hold on...')
        self._transport.queue('worker-job')
        self.send('worker-job', qname)
        
        w = SimpleWorker(self.options, [])
        t = time.time()
        self.assertRaises(hive.TimeoutError, w.main)
        t = time.time() - t
        self.assert_(t < 1.0, "%f < 1.0" % t)
        
    def testCentralNoWorkersTimeout(self):
        # Central w/no workers stops after timeout
        c = SimpleCentral(self.options, [])
        c.addstringseq('aaa', 'abc') # needs to have some work, or just exits!
        t = time.time()
        self.assertRaises(hive.TimeoutError, c.main)
        t = time.time() - t
        self.assert_(t < 1.0, "%f < 1.0" % t)

    def xtestCentralNoWorkersCantStartTimeout(self):
        # Central w/no workers stops after timeout, when stalled on starting
        c = SimpleCentral(self.options, [])
        c.addstringseq('aaa', 'abc') # needs to three units of work...
        c.addstringseq('bbb', 'def') 
        c.addstringseq('ccc', 'ghi') # ...so this one stalls on starting
        t = time.time()
        self.assertRaises(hive.TimeoutError, c.main)
        t = time.time() - t
        self.assert_(t < 1.0, "%f < 1.0" % t)



if __name__ == '__main__':
    unittest.main()

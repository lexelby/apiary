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

import os
import random
import time

import hive


class StatWorker(hive.Worker):
    def __init__(self, options, arguments):
        hive.Worker.__init__(self, options, arguments)
        self._events = 0
        self._bytes = 0
    
    def start(self):
        self._events = 0
        self._bytes = 0
    
    def event(self, data):
        self._events += 1
        self._bytes += len(data)
        #time.sleep(0.01)
        
    def end(self):
        return ("%s,%d,%d" % (self._id, self._events, self._bytes))
        

class StatCentral(hive.Central):
    def __init__(self, options, arguments):
        hive.Central.__init__(self, options, arguments)
        self.results = {}
        self._nseq = 2000
        self._nevents = 5
        self._nlen = 800
        self._seqcount = 0
        self._seqs = []
        self._seqevents = {}
        self._data = None
            
    def next(self):
        shy = self._seqcount < self._nseq
        empty = len(self._seqs) == 0
        if shy and (empty or random.randint(0,10) == 0):
            self._seqcount += 1
            seq = "Q%d" % (self._seqcount)
            self._seqs.append(seq)
            self._seqevents[seq] = 0
            self.start(seq)
            return True

        if empty:
            return False
        
        seq = random.choice(self._seqs)
        i = self._seqevents[seq]
        if i >= self._nevents:
            del self._seqevents[seq]
            self._seqs = self._seqevents.keys()
            self.end(seq)
            return True
        
        self._seqevents[seq] = i + 1
        self.event(seq, self.gendata())
        return True
    
    def result(self, seq, data):
        (worker, events, bytes) = data.split(',')
        if worker not in self.results:
            self.results[worker] = { 'sequences': 0, 'events': 0, 'bytes': 0 }
        r = self.results[worker]
        r['sequences'] += 1
        r['events'] += int(events)
        r['bytes'] += int(bytes)

    def gendata(self):
        if self._data is None:
            s = "abcdefghijklmnopqrstuvwxyz"
            while len(s) < self._nlen:
                s += s
            self._data = s[0:self._nlen]
        return self._data
        
    def main(self):
        t = - time.time()
        c = - time.clock()
        hive.Central.main(self)
        c += time.clock()
        t += time.time()

        print ("Timing: %f process clock, %f wall clock" % (c, t))
        print ("Central: %d sequences @ %d events @ %d bytes"
                % (self._seqcount, self._nevents, self._nlen))
        for k,r in self.results.iteritems():
            print ("Worker %s:" % k), (
                '%(sequences)d sequences @ %(events)d events @ %(bytes)d bytes'
                % r)


class StatHive(hive.Hive):
    def __init__(self):
        hive.Hive.__init__(self, StatCentral, StatWorker)


if __name__ == '__main__':
    StatHive().main()    

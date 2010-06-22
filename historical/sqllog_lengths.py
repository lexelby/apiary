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

import bisect
import sys

from sqllog import *
from stattools import StatValue


class SequenceLengths(FollowSequences):
    def __init__(self):
        FollowSequences.__init__(self)
        self.num_sequences = 0
        self.bucket_times = [ float(1<<i) for i in xrange(16) ]
        self.bucket_counts = [ 0 for i in self.bucket_times ]        
    
    def addingSequence(self, s, e):
        self.num_sequences += 1
    
    def notingEvent(self, s, e):
        pass
    
    def removingSequence(self, s, e):
        t = float(s.time())
        i = bisect.bisect_left(self.bucket_times, t)
        self.bucket_counts[i] += 1
            
    def writestats(self):
        print "%30s:   %8d" % ('num_sequences', self.num_sequences)
        print "Histogram of sequence lengths (log scale):"
        for i in xrange(len(self.bucket_times)):
            print "%30.1f:   %8d" % (self.bucket_times[i], self.bucket_counts[i])


if __name__ == '__main__':
    f = SequenceLengths()
    f.replay(input_events(sys.argv[1:]))
    f.writestats()

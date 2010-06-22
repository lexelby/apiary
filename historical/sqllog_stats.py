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

from sqllog import *
from stattools import StatValue


class StatSequences(FollowSequences):
    def __init__(self):
        FollowSequences.__init__(self)
        self.num_events = 0
        self.num_sequences = 0
        self.count_active_sequences = 0
        self.num_forced_end = 0
        self.stat_active_sequences = StatValue()
        self.stat_response_time = StatValue()
        self.stat_response_time_long = StatValue()
        self.stat_inter_query_time = StatValue()
        self.stat_inter_query_time_long = StatValue()
        self.stat_sequence_time = StatValue()
        self.stat_sequence_time_long = StatValue()
        
    
    def addingSequence(self, s, e):
        self.num_sequences += 1
        self.count_active_sequences += 1
    
    def notingEvent(self, s, e):
        self.num_events += 1
        self.stat_active_sequences.sample(self.count_active_sequences)
        
        stat_short = self.stat_inter_query_time
        stat_long = self.stat_inter_query_time_long        
        if e.state == Event.Response:
            stat_short = self.stat_response_time
            stat_long = self.stat_response_time_long

        t = s.timeto(e)
        if t is not None:
            t = float(t)
            if t < 1.0:
                stat_short.sample(t)
            else:
                stat_long.sample(t)
        pass
    
    def forcedEnd(self, s, e):
        self.num_forced_end += 1
        
    def removingSequence(self, s, e):
        self.count_active_sequences -= 1
        t = float(s.time())
        if t < 1.0:
            self.stat_sequence_time.sample(t)
        else:
            self.stat_sequence_time_long.sample(t)
            
    def format(self, stat):
        return stat.format("%8d", "%8.2f")
        
    def writestats(self):
        print "%30s:   %8d" % ('num_events', self.num_events)
        print "%30s:   %8d" % ('num_sequences', self.num_sequences)
        print "%30s:   %8d" % ('num_forced_end', self.num_forced_end)
        print "%30s: %s" % ('stat_active_sequences',
                                self.format(self.stat_active_sequences))
        print "%30s: %s" % ('stat_response_time <1s',
                                self.format(self.stat_response_time))
        print "%30s: %s" % ('stat_response_time_long >1s',
                                self.format(self.stat_response_time_long))
        print "%30s: %s" % ('stat_inter_query_time <1s',
                                self.format(self.stat_inter_query_time))
        print "%30s: %s" % ('stat_inter_query_time_long >1s',
                                self.format(self.stat_inter_query_time_long))
        print "%30s: %s" % ('stat_sequence_time <1s',
                                self.format(self.stat_sequence_time))
        print "%30s: %s" % ('stat_sequence_time >1s',
                                self.format(self.stat_sequence_time_long))




if __name__ == '__main__':
    f = StatSequences()
    f.replay(input_events(sys.argv[1:]))
    f.writestats()

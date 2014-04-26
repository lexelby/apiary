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
import cPickle

from sqllog import *

class GenerateJobs(CoalesceSequences):
    def __init__(self, *args, **kwargs):
        super(GenerateJobs, self).__init__(*args, **kwargs)
        
        self._base_time = None
    
    def fullSequence(self, e):
        # Jobs look like this:
        # (job_id, ((time, SQL), (time, SQL), ...))
        
        tasks = []
        
        for event in e.events():
            if event.state == event.Query:
                timestamp = float(event.time)
                
                if not self._base_time:
                    self._base_time = timestamp
                
                
                tasks.append((timestamp - self._base_time, event.body))
        
        job = (e.id, tuple(tasks))
        
        cPickle.dump(job, file=sys.stdout)
        

if __name__ == '__main__':
    f = GenerateJobs()
    f.replay(input_events(sys.argv[1:]))

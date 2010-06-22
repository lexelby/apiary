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

"""Simple statistics gathering tools

Classes:
    StatValue - accumulate statistics about a value

Exceptions:
    StatErrorNoSamples

"""

import math

class StatErrorNoSamples(Exception):
    """Raised when requesting a statistic on a value with no samples"""
    pass

class StatValue(object):
    """Accumulate statistics on a value
    
    Methods:
        sample - sample the variable
        count - return the number of samples
        min - return the minimum sample
        max - return the maximum sample
        average - return the average of samples
        stddev - return the standard deviation of samples
        format - return a string suitable for output
    
    """
    
    def __init__(self):
        self.reset()
    
    def _must_have_samples(self):
        if self._n == 0:
            raise StatErrorNoSamples()
    
    def reset(self):
        self._n = 0
        self._min = None
        self._max = None
        self._sum = 0.0
        self._sumsq = 0.0

    def sample(self, x):
        """Sample the variable"""
        self._n += 1
        if self._min is None or self._min > x:
            self._min = x
        if self._max is None or self._max < x:
            self._max = x
        self._sum += x
        self._sumsq += x*x
    
    def count(self):
        """Return the number of samples"""
        return self._n
    
    def min(self):
        """Return the minimum sample"""
        self._must_have_samples()
        return self._min
    
    def max(self):
        """Return the maximum sample"""
        self._must_have_samples()
        return self._max
    
    def average(self):
        """Return the average of samples"""
        self._must_have_samples()
        return self._sum / self._n
    
    def stddev(self):
        """Return the average of samples"""
        self._must_have_samples()
        avg = self._sum / self._n
        return math.sqrt(self._sumsq / self._n - avg*avg)
    
    def format(self, count_fmt="%6d", value_fmt="%12f"):
        """Return a string suitable for output
        
        The format will be five columns: count, min, avg., max, std.dev.,
        with some amount of punctuation separating them. For example:

            n=   3:  16.00,  42.63,  83.80, sd= 29.53        
        
        The numeric formats of the first column can be controlled by supplying
        a format string as the count_fmt argument. The format of the remaining
        columns is governed by the value_fmt argument. See the default values
        for examples.
        
        """
        
        if self._n == 0:
            return 'n=' + (count_fmt % 0) + ':'
            
        fmt = ("n=%s: %s, %s, %s, sd=%s" % 
                (count_fmt, value_fmt, value_fmt, value_fmt, value_fmt))
        return (fmt %
            (self._n, self._min, self.average(), self._max, self.stddev()))


class StatWindow(StatValue):
    def __init__(self, window):
        StatValue.__init__(self) # will call self.reset()
        self._window = window
    
    def reset(self):
        StatValue.reset(self)
        self._samples = []

    def _reduce_to(self, count):
        count_to_drop = self._n - count
        if count_to_drop <= 0:
            return
        if count_to_drop >= count:
            # faster to just replay the remaining samples
            samples = self._samples[count_to_drop:]
            self.reset()
            for x in samples:
                self.sample(x)
        else:
            # faster to undo the dropped samples
            dropped_min = False
            dropped_max = False
            for y in self._samples[:count_to_drop]:
                if y == self._min:
                    dropped_min = True
                if y == self._max:
                    dropped_max = True
                self._sum -= y
                self._sumsq -= y*y
            self._n -= count_to_drop
            self._samples = self._samples[count_to_drop:]
            if dropped_min:
                self._min = min(self._samples)
            if dropped_max:
                self._max = max(self._samples)
                        
    def setwindow(self, window):
        self._window = window
        self._reduce_to(window)
            
    def sample(self, x):
        self._reduce_to(self._window - 1)
        self._samples.append(x)
        StatValue.sample(self, x)

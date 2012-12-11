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

import math

class TimeStamp(object):

    def __init__(self, s=0, us=0):
        self.seconds = s
        self.micros = int(us)
    
        if type(s) is str:
            parts = s.split('.') # pylint: disable-msg=E1101
            n = len(parts)
            self.seconds = 0
            if n >= 1:
                self.seconds = int(parts[0])
            if n >= 2:
                us = int((parts[1] + "0000000")[0:7]) / 10.0
                self.micros = int(round(us))
        
        self._normalize()

    def __str__(self):
        return "%d.%06d" % (self.seconds, self.micros)

    def __repr__(self):
        return "TimeStamp(%d,%d)" % (self.seconds, self.micros)
    
    def __hash__(self):
        return hash(self.seconds) ^ hash(self.micros)
        
    def __cmp__(self, other):
        r = cmp(self.seconds, other.seconds)
        if r == 0:
            r = cmp(self.micros, other.micros)
        return r
    
    def __add__(self, other):
        s = self.seconds + other.seconds
        us = self.micros + other.micros
        if (us >= 1000000):
            us -= 1000000
            s += 1
        return TimeStamp(s, us)
        
    def __sub__(self, other):
        s = self.seconds - other.seconds
        us = self.micros - other.micros
        if (us < 0):
            us += 1000000
            s -= 1
        return TimeStamp(s, us)

    def __mul__(self, number):
        seconds = self.seconds * float(number)
        micros = self.micros * float(number)
        
        return TimeStamp(seconds, micros)
        
    def __float__(self):
        return self.seconds + self.micros / 1.0e6
    
    def _normalize(self):
        if type(self.seconds) is float:
            (frac_part, int_part) = math.modf(self.seconds)
            self.seconds = int(int_part)
            self.micros += int(round(frac_part * 1.0e6))        
        
        while self.micros > 1000000:
            self.micros -= 1000000
            self.seconds += 1
        
        while self.micros < -1000000:
            self.micros += 1000000
            self.seconds -= 1
        

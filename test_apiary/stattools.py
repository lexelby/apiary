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

import unittest

from apiary.tools.stattools import StatValue, StatWindow, StatErrorNoSamples


class TestValue(unittest.TestCase):
    
    def buildStat(self):
        return StatValue()
        
    def assertStats(self, s, count=None, min=None, max=None,
                            average=None, stddev=None):
        if count is not None:
            self.assertEqual(s.count(), count)
        if min is not None:
            self.assertEqual(s.min(), min)
        if max is not None:
            self.assertEqual(s.max(), max)        
        if average is not None:
            self.assertEqual(s.average(), average)        
        if stddev is not None:
            self.assertEqual(s.stddev(), stddev)

    def testEmpty(self):
        s = self.buildStat()
        self.assertEqual(s.count(), 0)
        self.assertRaises(StatErrorNoSamples, s.min)
        self.assertRaises(StatErrorNoSamples, s.max)        
        self.assertRaises(StatErrorNoSamples, s.average)        
        self.assertRaises(StatErrorNoSamples, s.stddev)        

    def testSingle(self):
        s = self.buildStat()
        s.sample(42)
        self.assertStats(s, count=1, min=42.0, max=42.0,
            average=42.0, stddev=0.0)

    atomic_weights = [15.9994, 83.798, 28.0855] # Carbon, Krypton, Silicon
    
    def testSequence(self):
        s = self.buildStat()
        for x in [2, 4, 4, 4, 5, 5, 7, 9]:
            s.sample(x)
        self.assertStats(s, count=8, min=2.0, max=9.0,
            average=5.0, stddev=2.0)

        s = self.buildStat()
        for x in self.atomic_weights:
            s.sample(x)
        self.assertStats(s, count=3, min=15.9994, max=83.798,
            average=42.627633333333328, stddev=29.527024592208562)

    def testFormat(self):
        s = self.buildStat()
        for x in [15.9994, 83.798, 28.0855]:
            s.sample(x)
        
        p = s.format("%4d", "%6.2f")
        self.assertEqual(p, "n=   3:  16.00,  42.63,  83.80, sd= 29.53")
    
        p = s.format("%d", "%8.4f")
        self.assertEqual(p, "n=3:  15.9994,  42.6276,  83.7980, sd= 29.5270")

    def testFormatEmtpy(self):
        s = self.buildStat()

        p = s.format("%4d", "%6.2f")
        self.assertEqual(p, "n=   0:")

        p = s.format("%d", "%8.4f")
        self.assertEqual(p, "n=0:")


class TestWindow(TestValue):
    def buildStat(self):
        return StatWindow(10)

    def testSliding(self):
        s = StatWindow(3)
        s.sample(15)
        s.sample(9)
        s.sample(6)
        self.assertStats(s, count=3, min=6, max=15, average=10.0)
        s.sample(12)
        self.assertStats(s, count=3, min=6, max=12, average=9.0)
        s.sample(12)
        self.assertStats(s, count=3, min=6, max=12, average=10.0)
        s.sample(9)
        self.assertStats(s, count=3, min=9, max=12, average=11.0)
    
    def testResizing(self):
        s = StatWindow(6)
        for x in [ 2, 6, 3, 8, 5, 3 ]:
            s.sample(x)
        self.assertStats(s, count=6, min=2, max=8, average=4.5)
        s.setwindow(2) # less than half
        self.assertStats(s, count=2, min=3, max=5, average=4.0)
        
        s = StatWindow(6)
        for x in [ 2, 6, 3, 8, 5, 3 ]:
            s.sample(x)
        self.assertStats(s, count=6, min=2, max=8, average=4.5)
        s.setwindow(4) # more than half
        self.assertStats(s, count=4, min=3, max=8, average=4.75)
        
        
          
if __name__ == '__main__':
    unittest.main()

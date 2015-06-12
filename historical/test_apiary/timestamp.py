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

from apiary.tools.timestamp import TimeStamp


class TestConstruction(unittest.TestCase):

    def testFloatConstruction(self):
        ts = TimeStamp(12.3456789)
        self.assertEqual(ts.seconds, 12)
        self.assertEqual(ts.micros, 345679)

        ts = TimeStamp(123.456789)
        self.assertEqual(ts.seconds, 123)
        self.assertEqual(ts.micros, 456789)

        ts = TimeStamp(1234.56789)
        self.assertEqual(ts.seconds, 1234)
        self.assertEqual(ts.micros, 567890)

        ts = TimeStamp(12345.6789)
        self.assertEqual(ts.seconds, 12345)
        self.assertEqual(ts.micros, 678900)

        ts = TimeStamp(123456.789)
        self.assertEqual(ts.seconds, 123456)
        self.assertEqual(ts.micros, 789000)

        ts = TimeStamp(1234567.89)
        self.assertEqual(ts.seconds, 1234567)
        self.assertEqual(ts.micros, 890000)

        ts = TimeStamp(12345678.9)
        self.assertEqual(ts.seconds, 12345678)
        self.assertEqual(ts.micros, 900000)

        ts = TimeStamp(123456789.)
        self.assertEqual(ts.seconds, 123456789)
        self.assertEqual(ts.micros, 0)
        
    def testIntConstruction(self):
        ts = TimeStamp(123456789)
        self.assertEqual(ts.seconds, 123456789)
        self.assertEqual(ts.micros, 0)

        ts = TimeStamp(123, 456789)
        self.assertEqual(ts.seconds, 123)
        self.assertEqual(ts.micros, 456789)

    def testStringConstruction(self):

        ts = TimeStamp('0.123456')
        self.assertEqual(ts.seconds, 0)
        self.assertEqual(ts.micros, 123456)

        ts = TimeStamp('12.3456789')
        self.assertEqual(ts.seconds, 12)
        self.assertEqual(ts.micros, 345679)

        ts = TimeStamp('123.456789')
        self.assertEqual(ts.seconds, 123)
        self.assertEqual(ts.micros, 456789)

        ts = TimeStamp('1234.56789')
        self.assertEqual(ts.seconds, 1234)
        self.assertEqual(ts.micros, 567890)

        ts = TimeStamp('12345.6789')
        self.assertEqual(ts.seconds, 12345)
        self.assertEqual(ts.micros, 678900)

        ts = TimeStamp('123456.789')
        self.assertEqual(ts.seconds, 123456)
        self.assertEqual(ts.micros, 789000)

        ts = TimeStamp('1234567.89')
        self.assertEqual(ts.seconds, 1234567)
        self.assertEqual(ts.micros, 890000)

        ts = TimeStamp('12345678.9')
        self.assertEqual(ts.seconds, 12345678)
        self.assertEqual(ts.micros, 900000)

        ts = TimeStamp('123456789.')
        self.assertEqual(ts.seconds, 123456789)
        self.assertEqual(ts.micros, 0)


class TestOperations(unittest.TestCase):

    def testEquality(self):
        t1 = TimeStamp(100, 900000)
        t1x = TimeStamp(100.9)
        self.assert_(t1 == t1x)
        
    def testOrdering(self):
        t1 = TimeStamp(100, 900000)
        t2 = TimeStamp(100, 900001)
        t3 = TimeStamp(101, 000002)
        
        self.assert_(t1 < t2)
        self.assert_(t2 < t3)
        self.assert_(t1 < t3)
        self.assert_(t3 > t1)
    
    def testDifference(self):
        t1 = TimeStamp(100, 900000)
        t2 = TimeStamp(100, 900001)
        t3 = TimeStamp(101, 000002)
        t1x = TimeStamp(100.9)

        self.assertEqual(t1 - t1x, TimeStamp(0,0))
        
        self.assertEqual(t2 - t1, TimeStamp(0,1))
        self.assertEqual(t3 - t2, TimeStamp(0,100001))
        self.assertEqual(t3 - t1, TimeStamp(0,100002))

        self.assertEqual(t1 - t2, TimeStamp(-1,999999))
        self.assertEqual(t2 - t3, TimeStamp(-1,899999))
        self.assertEqual(t1 - t3, TimeStamp(-1,899998))

    def testAdding(self):
        t1 = TimeStamp(100, 900000)
        t2 = TimeStamp(100, 900001)
        t3 = TimeStamp(101, 0)
        t4 = TimeStamp(101, 000002)

        d0 = TimeStamp(0,0)
        d21 = TimeStamp(0,1)
        d31 = TimeStamp(0,100000)
        d41 = TimeStamp(0,100002)
        d42 = TimeStamp(0,100001)
        
        self.assertEqual(t1 + d0, t1)
        self.assertEqual(t1 + d21, t2)
        self.assertEqual(t1 + d31, t3)
        self.assertEqual(t1 + d41, t4)
        self.assertEqual(t2 + d42, t4)
    
    def testAddInPlace(self):
        t1 = TimeStamp(100, 900000)
        t1 += TimeStamp(1,1)
        self.assertEqual(t1, TimeStamp(101, 900001))
        
        
class TestStringConversion(unittest.TestCase):

    def testStr(self):
        self.assertEqual(str(TimeStamp(100,0)), '100.000000')
        self.assertEqual(str(TimeStamp(100,1)), '100.000001')
        self.assertEqual(str(TimeStamp(0)), '0.000000')

    def testRepr(self):
        self.assertEqual(repr(TimeStamp(100,0)), 'TimeStamp(100,0)')
        self.assertEqual(repr(TimeStamp(100,1)), 'TimeStamp(100,1)')
        self.assertEqual(repr(TimeStamp(0)), 'TimeStamp(0,0)')
    
    def testFloat(self):
        self.assertEqual(float(TimeStamp(100,0)), 100.0)
        self.assertEqual(float(TimeStamp(100,1)), 100.000001)
        self.assertEqual(float(TimeStamp(100,499999)), 100.499999)
        self.assertEqual(float(TimeStamp(100,500000)), 100.500000)
        self.assertEqual(float(TimeStamp(100,500001)), 100.500001)
        self.assertEqual(float(TimeStamp(100,999999)), 100.999999)
        self.assertEqual(float(TimeStamp(0)), 0.0)

class TestRanges(unittest.TestCase):
    
    def testBig(self):
        ts = TimeStamp('1237237351.064861')
        self.assertEqual(ts.seconds, 1237237351)
        self.assertEqual(ts.micros, 64861)



if __name__ == '__main__':
    unittest.main()

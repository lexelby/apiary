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

from mergetools import merge, imerge

class TestMerge(unittest.TestCase):

    def testBasics(self):
        a = merge([1, 3, 5], [2, 4])
        self.assertEqual(a, [1, 2, 3, 4, 5])
        
        a = merge([1, 3, 5])
        self.assertEqual(a, [1, 3, 5])
    

    def testEmpty(self):
        a = merge()
        self.assertEqual(a, [])
        
        a = merge([])
        self.assertEqual(a, [])
        
        a = merge([], [])
        self.assertEqual(a, [])

        a = merge([], [], [])
        self.assertEqual(a, [])

        a = merge([6], [])
        self.assertEqual(a, [6])

        a = merge([], [6])
        self.assertEqual(a, [6])


    def testShort(self):
        a = merge([4], [1, 3, 5], [2, 6])
        self.assertEqual(a, [1, 2, 3, 4, 5, 6])
        
        a = merge([1, 3, 5], [4], [2, 6])
        self.assertEqual(a, [1, 2, 3, 4, 5, 6])
        
        a = merge([1, 3, 5], [2, 6], [4])
        self.assertEqual(a, [1, 2, 3, 4, 5, 6])


    def testMergeOverIters(self):
        a = merge(xrange(5, 21, 5), xrange(3, 21, 3), xrange(7, 21, 7))
        self.assertEqual(a, [3, 5, 6, 7, 9, 10, 12, 14, 15, 15, 18, 20])


class Thing(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value
    
    def __lt__(self, other):
        return self.value < other.value
    
    def __str__(self):
        return self.name + "-" + str(self.value)


class TestOrdering(unittest.TestCase):

    def testDuplicates(self):
        a = merge([Thing('amy', 3), Thing('bob', 3)],
                    [Thing('cy', 2), Thing('dawn', 4)])
        s = ','.join(map(str, a))
        self.assertEqual(s, 'cy-2,amy-3,bob-3,dawn-4')
        
        a = merge([Thing('amy', 3), Thing('bob', 3), Thing('harry', 5)], 
                    [Thing('cy', 2), Thing('guy', 3), Thing('dawn', 4)])
        s = ','.join(map(str, a))
        self.assertEqual(s, 'cy-2,amy-3,bob-3,guy-3,dawn-4,harry-5')
        
        
    
  
if __name__ == '__main__':
    unittest.main()

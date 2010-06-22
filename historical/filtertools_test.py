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

from filtertools import filterthru

def f_id(x):
    return [x]

def f_rev(x):
    return [''.join(map(None, reversed(x)))]

def f_csv(x):
    return x.split(',')

def f_xxx(x):
    if x[0] == "x":
        return []
    return [x]

class TestFilter(unittest.TestCase):

    def assertNoChange(self, input, stack):
        output = filterthru(input, stack)
        if type(input) != list:
            self.assertEqual(output, [input])
        else:
            self.assertEqual(output, input)
        
    def testEmptyStack(self):
        self.assertNoChange([], [])
        self.assertNoChange(["abc"], [])
        self.assertNoChange(["abc", "def"], [])
    
    def testFilterId(self):
        self.assertNoChange([], [f_id])
        self.assertNoChange(["abc"], [f_id])
        self.assertNoChange(["abc", "def"], [f_id])

        self.assertNoChange([], [f_id, f_id])
        self.assertNoChange(["abc"], [f_id, f_id])
        self.assertNoChange(["abc", "def"], [f_id, f_id])

    def testModify(self):
        self.assertEqual(filterthru(["abc"], [f_rev]), ["cba"])
        self.assertEqual(filterthru(["abc", "def"], [f_rev]), ["cba", "fed"])

        self.assertNoChange(["abc"], [f_rev, f_rev])
        self.assertNoChange(["abc", "def"], [f_rev, f_rev])

    def testExpand(self):
        self.assertEqual(filterthru(["abc"], [f_csv]), ["abc"])
        self.assertEqual(filterthru(["abc,def"], [f_csv]), ["abc", "def"])
        self.assertEqual(filterthru(["abc,def,ghi", "jkl,mno"], [f_csv]),
            ["abc", "def", "ghi", "jkl", "mno"])
    
    def testRemove(self):
        self.assertEqual(filterthru(["abc"], [f_xxx]), ["abc"])
        self.assertEqual(filterthru(["xyz"], [f_xxx]), [])
        self.assertEqual(filterthru(["abc", "xyz"], [f_xxx]), ["abc"])
        self.assertEqual(filterthru(["xyz", "abc"], [f_xxx]), ["abc"])
    
    def testChain(self):
        self.assertEqual(filterthru(["abc,def,xyz"], [f_csv, f_xxx]),
            ["abc", "def"])
        self.assertEqual(filterthru(["abc,def,xyz"], [f_xxx, f_csv]),
            ["abc", "def", "xyz"])
        self.assertEqual(filterthru(["abc,def,xyz"], [f_rev, f_csv, f_xxx]),
            ["zyx", "fed", "cba"])
        self.assertEqual(filterthru(["abc,def,xyz"], [f_csv, f_rev, f_xxx]),
            ["cba", "fed", "zyx"])
        self.assertEqual(filterthru(["abc,def,xyz"], [f_csv, f_xxx, f_rev]),
            ["cba", "fed"])
        self.assertEqual(filterthru(["xxx,xyz"], [f_csv, f_xxx, f_rev]),
            [])

  
if __name__ == '__main__':
    unittest.main()
              

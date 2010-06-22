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

import re
import unittest

class TestREBuilding(unittest.TestCase):

    def testBasicPlan(self):
        q = re.compile(r'[^`.\w]`?(?:foo|bar|baz)`?(?:[^`.\w]|$)')
        self.assert_(q.search('FROM foo,'))
        self.assert_(q.search('FROM bar,'))
        self.assert_(q.search('FROM baz,'))
        self.assert_(not q.search('FROM moo,'))
        self.assert_(not q.search('FROM ffoo,'))
        self.assert_(not q.search('FROM bazz,'))
        self.assert_(q.search('FROM `foo`,'))
        self.assert_(q.search('FROM `bar`,'))
        self.assert_(q.search('FROM `baz`,'))
        self.assert_(not q.search('FROM `moo`,'))
        self.assert_(not q.search('FROM `ffoo`,'))
        self.assert_(not q.search('FROM `bazz`,'))

        self.assert_(q.search('FROM foo'))
        self.assert_(q.search('(foo as f'))
        self.assert_(not q.search('FROM schema.foo'))
        self.assert_(not q.search('FROM `schema`.`foo`'))
        self.assert_(not q.search('SELECT foo.col'))
        self.assert_(not q.search('SELECT `foo`.`col`'))

if __name__ == '__main__':
    unittest.main()

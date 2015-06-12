#! /usr/bin/env python
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

import os
import unittest

import test_apiary


class ComprehensiveTestLoader (unittest.TestLoader):
    # Sheesh...  I can't believe we have to write this class to discover all tests in a package:
    def loadTestsFromModule(self, module):
        def import_submodule(modname):
            m = __import__(modname)
            for frag in modname.split('.')[1:]:
                m = getattr(m, frag)
            return m
            
        path = module.__file__
        if os.path.splitext(os.path.basename(path))[0] == '__init__':
            # This is a package, rather than a simple module:
            suites = []
            
            for name in os.listdir(os.path.dirname(path)):
                stem, ext = os.path.splitext(name)
                if stem == '__init__':
                    continue
                elif ext == '.py':
                    m = import_submodule(module.__name__ + '.' + stem)
                    suites.append(self.loadTestsFromModule(m))
                elif ext == '':
                    subpath = os.path.join(path, name)
                    if os.path.isdir(subpath):
                        # *FIX: This may fail if there is a non-package subdirectory:
                        m = import_submodule(module.__name__ + '.' + name)
                        suites.append(self.loadTestsFromModule(m))
                else:
                    pass # Ignore other file types.
            return self.suiteClass(suites)
        else:
            # Simple superclass loader handles a single module:
            return unittest.TestLoader.loadTestsFromModule(self, module)
                    


if __name__ == '__main__':
    unittest.main(module=test_apiary, testLoader=ComprehensiveTestLoader())

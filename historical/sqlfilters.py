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
import sys
import time

from sqllog import *

def build_matchers(path='indra-tables'):
    schemas = {}
    tableRE = re.compile(r'(\w+)\.(\w+)')
    for line in file(path):
        m = tableRE.match(line)
        if m:
            schema, table = m.groups()
            if schema not in schemas:
                schemas[schema] = []
            schemas[schema].append(table)
    matchers = {}
    for schema,table_list in schemas.iteritems():
        expr = r'[^`.\w]`?(?:' + '|'.join(table_list) + r')`?(?:[^`.\w]|$)'
        matchers[schema] = re.compile(expr)
    return matchers



class PrependSchema(object):
    def __init__(self, default_schema=None):
        self._matchers = build_matchers()
        self._default_schema = default_schema

    def __call__(self, sql):
        n = 0
        s = []
        for schema,matcher in self._matchers.iteritems():
            if matcher.search(sql):
                n += 1
                s = schema
                
        if n == 1 and s != self._default_schema:
            return ["USE " + s, sql]
        return [sql]





class FindMissingSchemas(FollowSequences):
    def __init__(self):
        FollowSequences.__init__(self)
        self._matchers = build_matchers()
        self._num_full_spec = 0
        self._num_add_schema = {}
        self._num_add_multiple = 0
    
    def notingEvent(self, s, e):
        if e.state != Event.Query:
            return
        n = 0
        s = []
        for schema,matcher in self._matchers.iteritems():
            if matcher.search(e.body):
                n += 1
                s.append(schema)
                
        if n == 0:
            self._num_full_spec += 1
        elif n == 1:
            self._num_add_schema[s[0]] = self._num_add_schema.get(s[0], 0) + 1
        else:
            self._num_add_multiple += 1
            
        if True and n > 0:
            if n > 1:
                print "*** TWO MATCHES ***"
            print "USE", ','.join(s)
            print e.body
            print '----------------------------------'

    def report(self):
        print "%30s:   %8d" % ("_num_full_spec", self._num_full_spec)
        schemas = self._num_add_schema.keys()
        schemas.sort()
        for s in schemas:
            print "%20s %9s:   %8d" % ("_num_add_schema", s, self._num_add_schema[s])
        print "%30s:   %8d" % ("_num_add_multiple", self._num_add_multiple)
        
        
        
if __name__ == '__main__':
    f = FindMissingSchemas()

    t = - time.time()
    c = - time.clock()
    f.replay(input_events(sys.argv[1:]))
    c += time.clock()
    t += time.time()

    print ("Timing: %f process clock, %f wall clock" % (c, t))
    f.report()


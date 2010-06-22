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

_DEBUG = False

def enable_debug():
    global _DEBUG
    _DEBUG = True
    debug('Debug enabled.')
    
def debug(tmpl, *args):
    if _DEBUG:
        print '[DEBUG] ' + (tmpl % args)


def traced_func(f):
    name = f.__name__
    def wrapped(*a, **kw):
        return trace_call(name, f, *a, **kw)
    return wrapped
    

def traced_method(m):
    def wrapped(self, *a, **kw):
        name = self.__class__.__name__ + '.' + m.__name__
        return trace_call(name, m, self, *a, **kw)
    return wrapped


def trace_call(name, f, *a, **kw):
    debug('Call: %s%r %r', name, a, kw)
    try:
        r = f(*a, **kw)
    except Exception, e:
        debug('Raise: %s !-> %r %r', name, e, e.args)
        raise
    debug('Return: %s -> %r', name, r)
    return r

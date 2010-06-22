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

"""mergetools - Merge together sorted sequences

Functions:
imerge - Return an iterator that produces the merge of the arguments
merge - Return an array that is the merge of the arguments

In both cases, the arguments can be zero or more iterable objects. Collections,
iterators, and any object implementing the iterator protocol are acceptable.

If the sequences being merged are not already in sorted order, then the result
order is not well defined.

Items in the sequences are compared using the < operator. Duplicate items are
allowed, and will be returned as many times as they appear. If duplicate items
appear in more than one of the input sequences, then they will appear in the
output in the order of the input sequences.
"""

__all__ = [
        'imerge',
        'merge',
    ]

class _Feed(object):
    def __init__(self, iterable):
        self._iteration = iter(iterable)
        self._head = None
        self.advance()
    
    def empty(self):
        return self._iteration is None
    
    def head(self):
        return self._head
        
    def advance(self):
        try:
            self._head = self._iteration.next()
        except StopIteration:
            self._iteration = None
            self._head = None

    
class _IMerge(object):
    def __init__(self, iterables):
        self._feeds = map(_Feed, iterables)
    
    def __iter__(self):
        return self
    
    def next(self):
        source = None
        smallest = None
        for f in self._feeds:
            if f.empty():
                continue
            if source is None or f.head() < smallest:
                source = f
                smallest = f.head()
        if source is None:
            raise StopIteration
        source.advance()
        return smallest
        

def imerge(*iterables):
    """Return an iterator that produces the merge of the arguments"""
    return _IMerge(iterables)

def merge(*iterables):
    """Return an array that is the merge of the arguments"""
    return map(None, _IMerge(iterables))

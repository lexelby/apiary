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


'''
A span represents an ordered pair.  This module contains algorithms for selecting windows of span by start, end, or overlap.
'''
import bisect


class Span (tuple):
    '''
    A Span is a specialized pair representing the partially open range [start, end) along with application associated data.

    It includes the start, but not the end. Note: This implies a Span cannot represent a point.
    '''
    def __new__(cls, start, end, data=None):
        assert start <= end, `start, end` # *FIX: Can magnitude be 0?  Before we disallowed, but now we do for demo convenience.
        return tuple.__new__(cls, (start, end))

    def __init__(self, start, end, data=None):
        self.data = data
        
    def __repr__(self):
        return '<%s (%r,%r) %r>' % (self.__class__.__name__,
                                    self.start,
                                    self.end,
                                    self.data)
    
    @property
    def start(self):
        return self[0]

    @property
    def end(self):
        return self[1]

    @property
    def magnitude(self):
        lo, hi = self
        return hi - lo
    
    def contains(self, point):
        return self.start <= point < self.end
    
    def overlaps(self, other):
        assert isinstance(other, Span), `self, other`
        return self.contains(other.start) \
               or (other.end > self.start and self.contains(other.end)) \
               or other.contains(self.start) \
               or (self.end > other.start and other.contains(self.end))
    

class SpanSequence (object):
    '''
    A SpanSequence is a sequence of spans ordered by start and end points.
    '''
    def __init__(self, spans=[]):
        self._spans = []
        for s in spans:
            self.insert(s)

    def __iter__(self):
        return iter(self._spans)
    
    def __cmp__(self, other):
        return cmp(self._spans, other._spans)
    
    def __len__(self):
        return len(self._spans)
    
    def insert(self, span):
        '''
        Slow unsorted insert.  O(log N)
        '''
        assert isinstance(span, Span), `self, span`
        i = bisect.bisect(self._spans, span)
        if i > 0 and self._spans[i-1] == span:
            # *HACK: skip duplicate insert.  Need to nail down a bug here.  See *FIX comments in base.py and above.
            assert self._spans[i-1] <= span, `i, self._spans[i-1], span`
            return
        elif i < len(self._spans) and self._spans[i] == span:
            # *HACK: skip duplicate insert.  Need to nail down a bug here.  See *FIX comments in base.py and above.
            assert self._spans[i] >= span, `i, self._spans[i], span`
            return
        self._spans.insert(i, span)

    def append(self, span):
        '''
        Fast sorted append.  The span argument must come at the end of this sequence.
        '''
        if self._spans:
            if self._spans[-1] == span:
                # *HACK: skip duplicate insert.  Need to nail down a bug here.  See *FIX comments in base.py and above.
                return
            assert span > self._spans[-1], `self._spans[-1], span`
        self._spans.append(span)

    def as_bins(self, binwidth=1.0):
        if self._spans:
            s = self._spans[0].start
            window = Span(s, s+binwidth)
            subseq = SpanSequence()
            for span in self:
                while not window.contains(span.start):
                    yield window, subseq
                    subseq = SpanSequence()
                    window = Span(window.end, window.end + binwidth)
                subseq.append(span)
            yield window, subseq
                    
    def concurrency_vector(self):
        '''
        Yield an ordered sequence of (t, span, spans) where spans consists
        of all spans which overlap t.  The span is either newly added
        to spans if just starting, or just removed if ending.  The t
        value is either span.start or span.end.
        '''
        q = [] # Contains a sorted list of (span.end, span)

        for span in self:
            while q and q[0][0] <= span.start:
                # Decrease concurrency:
                end, other = q.pop(0)
                yield (end, other, list(q))
            bisect.insort(q, (span.end, span))
            yield (span.start, span, list(q))

        while q:
            end, span = q.pop(0)
            yield (end, span, list(q))


class SlidingWindowSequence (SpanSequence):
    '''
    A SlidingWindowSequence is a SpanSequence and ensures that it extends no more than some cutoff into the past.
    '''
    def __init__(self, width):
        SpanSequence.__init__(self)
        self.width = width
        
    def insert(self, span):
        SpanSequence.insert(self, span)
        self._enforce_cutoff()
        
    def append(self, span):
        SpanSequence.append(self, span)
        self._enforce_cutoff()

    def _enforce_cutoff(self):
        end = self._spans[-1].start
        cutoff = end - self.width
        
        for i, s in enumerate(self):
            if s.start > cutoff:
                break

        self._spans = self._spans[i:]

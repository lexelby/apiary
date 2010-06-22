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
from apiary.tools.span import Span, SpanSequence, SlidingWindowSequence
    

class SpanTests (unittest.TestCase):
    def testInvalidRangeConstruction(self):
        # *FIX: We allow point spans for now for the convenience of a demo:
        #badspans = [(0, 0), # A Span cannot be a point.
        #            (1, 0)]
        badspans = [(1, 0)]
        
        for badspan in badspans:
            l, r = badspan
            try:
                span = Span(l, r)
            except AssertionError:
                return # Success
            self.fail('Invalid span range %r successfully constructed span %r.' % (badspan, span))
        
    def testStartAndEndInvariants(self):
        li, ri = 0, 1
        span = Span(li, ri)
        self.failUnlessEqual(li, span.start)
        self.failUnlessEqual(ri, span.end)
        lo, ro = span
        self.failUnlessEqual(li, lo)
        self.failUnlessEqual(ri, ro)
        self.failUnlessEqual(span.start, lo)
        self.failUnlessEqual(span.end, ro)

    def testMagnitudeInvariants(self):
        for (l, r) in [(0, 1), (0, 10), (-100, 100)]:
            span = Span(l, r)
            self.failUnlessEqual(span.magnitude, r-l)
            
    def testStartAndEndReadonly(self):
        span = Span(0, 1)
        err = None
        try:
            span.start = 42
        except AttributeError, e:
            err = e
        self.failIfEqual(None, err)
        err = None
        try:
            span.end = 43
        except AttributeError, e:
            err = e
        self.failIfEqual(None, err)

    def testPositiveContains(self):
        span = Span(0, 10)
        for i in range(span.start, span.end):
            self.failUnless(span.contains(i),
                            '%r should contain %r but .contains() returns False.' % (span, i))

    def testNegativeContains(self):
        span = Span(0, 10)
        for badpt in [span.end, span.start - 0.1, 2**33, -2**33]:
            self.failIf(span.contains(badpt),
                        '%r should not contain %r but .contains() returns True.' % (span, badpt))
        
    def testPositiveOverlaps(self):
        span = Span(0, 10)
        for (s, e) in [(9.9, 10), span, (-42, 0.1), (9.99999, 43), (-10, 20), (5, 6)]:
            olap = Span(s, e)
            self.failUnless(span.overlaps(olap),
                            '%r should overlap %r, but .overlaps returns False.' % (span, olap))

    def testNegativeOverlaps(self):
        span = Span(0, 10)
        for (s, e) in [(-10, 0), (10, 11), (-42, -0.000001), (10, 43)]:
            olap = Span(s, e)
            self.failIf(span.overlaps(olap),
                        '%r should not overlap %r, but .overlaps returns True.' % (span, olap))


def make_span(tup):
    '''Make a Span from a tuple and associate an arbitrary but instance-specific data item.'''
    (s, e) = tup
    return Span(s, e, data=('somedata', s, e))


class SpanSequenceTests (unittest.TestCase):
    empty = []
    
    contiguous = map(make_span,
                     [(0, 1),
                      (1, 2),
                      (2, 3)])

    discontiguous = map(make_span,
                        [(0, 1),
                         (2, 3),
                         (4, 5)])

    overlapping = map(make_span,
                      [(0, 2),
                       (1, 3),
                       (1, 4),
                       (2, 5)])

    all_sequences = [empty, contiguous, discontiguous, overlapping]


    def testData(self):
        for seq in self.all_sequences:
            seq = SpanSequence(seq)
            for span in seq:
                self.failUnlessEqual(span.data, ('somedata', span.start, span.end))
        
    def testOrdering(self):
        for seq in self.all_sequences:
            seq = SpanSequence(seq)
            lastspan = None
            for span in seq:
                if lastspan is None:
                    lastspan = span.start
                else:
                    self.failUnless(lastspan < span)
                    lastspan = span

    def testPositiveAppend(self):
        for spans in self.all_sequences:
            seq = SpanSequence()
            for span in spans:
                seq.append(span)
                    
    def testNegativeAppend(self):
        for spans in self.all_sequences:
            seq = SpanSequence()
            for span in reversed(spans):
                try:
                    seq.append(span)
                except AssertionError:
                    pass # Successfully detected out-of-order append.
        
    def testInsertAndAppendEquivalence(self):
        for spans in self.all_sequences:
            ins = SpanSequence(spans)
            app = SpanSequence()
            [app.append(s) for s in spans]
            self.failUnlessEqual(ins, app)
        
    def testLength(self):
        for spans in self.all_sequences:
            seq = SpanSequence(spans)
            self.failUnlessEqual(len(seq), len(spans))

    def testDisjointBins(self):
        for spans in [self.empty, self.contiguous, self.discontiguous]: # Exclude overlapping for the test condition:
            if spans:
                disjointbinwidth = min([s.magnitude for s in spans])
            else:
                disjointbinwidth = 1 # The value does not matter, so long as positive.
            seq = SpanSequence(spans)
            for window, subseq in seq.as_bins(disjointbinwidth):
                if len(subseq) not in (0, 1):
                    self.fail('Disjoint bin over %r contains more than one span: %r' % (window, list(subseq)))

    def testDisjointUniformBins(self):
        '''In this test, every bin should have exactly one span.'''
        seq = SpanSequence(self.contiguous)
        for window, subseq in seq.as_bins(self.contiguous[0].magnitude):
            self.failUnlessEqual(1, len(subseq),
                                 'Uniform disjoint bin over %r does not contain a single span:: %r' % (window, list(subseq)))  

    def testConcurrencyVectorTimesIncreaseMonotonically(self):
        decreasing_endings = map(make_span, [(0, 2), (0, 1), (3, 4)])
        last = None
        for (t, span, spans) in SpanSequence(decreasing_endings).concurrency_vector():
            if last is not None:
                self.failIf(last > t,
                            'Overlap vector is not monotonically increasing.')
            last = t
                        
        


class SlidingWindowSequenceTests (unittest.TestCase):
    def testDiscontinuousWindow(self):
        for spans in [SpanSequenceTests.contiguous,
                      SpanSequenceTests.discontiguous]:

            slwin = SlidingWindowSequence(spans[1].start - spans[0].start)
            for span in spans:
                slwin.append(span)
                self.failUnlessEqual(1, len(slwin),
                                     'Sliding window should encompass exactly one span, not: %r' % (slwin._spans,))
            

        
if __name__ == '__main__':
    unittest.main()

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

from StringIO import StringIO
import unittest

from apiary.mysql.sqllog import *
from apiary.tools.timestamp import TimeStamp
    

TestSource = 'unittest'
FakeJobs = [str(n) for n in range(111, 1000, 111)]
FakeJob = FakeJobs[-1]


class TestEvent(unittest.TestCase):

    def testEventStr(self):
        e = Event(123456.789, FakeJobs[0], TestSource, 'QueryStart', 'SELECT * FROM foo')
        self.assertEqual(str(e), """\
123456.789000\t%s\t%s\tQueryStart
SELECT * FROM foo
**************************************
""" % (FakeJobs[0], TestSource))

    def testOrdering(self):
        e1 = Event(10001.700, FakeJob, TestSource, 'QueryStart', 'SELECT')
        e2 = Event(10001.717, FakeJob, TestSource, 'QueryResponse', 'SELECT')
        e3 = Event(10001.717, FakeJob, TestSource, 'Quit', 'Quit')
        e4 = Event(10001.729, FakeJobs[0], TestSource, 'QueryStart', 'SELECT')
        
        self.assert_(e1 < e2)
        self.assert_(e1 < e3)
        self.assert_(e1 < e4)
        self.assert_(e2 < e3)
        self.assert_(e2 < e4)
        self.assert_(e3 < e4)


class TestParsing(unittest.TestCase):

    def testSimpleStanza(self):
        f = StringIO("""\
1237237351.064861\t10.0.0.1:40784\t%s\tQueryResponse
SELECT column1, column2 FROM some_table WHERE column1='foo'
**************************************
1237237351.065393\t10.0.0.2:39706\t%s\tQueryStart
SELECT t1.column1, t2.column2, t1.column3  FROM table1 t1, table2 t2  WHERE t1.column1 = '00000000-0000-0000-0000-000000000000'  AND t2.column2 = t1.column4
**************************************
""" % (TestSource, TestSource))
        s = parse_stanza(f)
        self.assert_(s is not None)
        self.assertEqual(s.time, TimeStamp(1237237351.064861))
        self.assertEqual(s.id, '10.0.0.1:40784')
        self.assertEqual(s.state, 'QueryResponse')
        self.assertEqual(s.body, """\
SELECT column1, column2 FROM some_table WHERE column1='foo'
""")

    def disabled_testMkQueryLogSyntax(self):
        f = StringIO("""\
# administrator command: Connect;
# Time: 091022 12:43:08.898136
# User@Host: user[user] @ 10.0.0.1 []
# Client: 10.0.0.1:40737
# Thread_id: 10000000
# Query_time: 0  Lock_time: 0  Rows_sent: 0  Rows_examined: 0
use some_table;
SELECT foo FROM bar WHERE column1 = 'some_uid' AND column2 = 'another_uid' AND column3 = 1;
""")
        s = parse_stanza(f)
        self.assert_(s is not None)
        self.assertEqual(s.time, TimeStamp(1256215388.898136))
        self.assertEqual(s.id, '10.0.0.1:40737:10000000')
        self.assertEqual(s.body, """\
use some_table;
SELECT foo FROM bar WHERE column1 = 'some_uid' AND column2 = 'another_uid' AND column3 = 1;
""")

    def testEmptyStanza(self):
        f = StringIO('')
        s = parse_stanza(f)
        self.assert_(s is None)
    
    def testMissingStanzaEnd(self):
        f = StringIO("""\
1237237351.064861\t10.0.0.1:40784\t%s\tQueryResponse
SELECT column1, column2 FROM table1 WHERE column3='foo'
""" % TestSource)
        s = parse_stanza(f)
        self.assert_(s is not None)
        self.assertEqual(s.time, TimeStamp(1237237351.064861))
        self.assertEqual(s.id, '10.0.0.1:40784')
        self.assertEqual(s.state, 'QueryResponse')
        self.assertEqual(s.body, """\
SELECT column1, column2 FROM table1 WHERE column3='foo'
""")

    def testJunkLeadInStanza(self):
        f = StringIO("""\
SELECT t1.column1, t2.column2, t1.column3 FROM table1 t1, table2 t2  WHERE t1.column4 = 'foo'  AND t2.column5 = u.column6
**************************************
1237237351.064861\t10.0.0.1:40784\t%s\tQueryResponse
SELECT column1, column2 FROM table1 WHERE column3='foo'
**************************************
""" % TestSource)
        s = parse_stanza(f)
        self.assert_(s is not None)
        self.assertEqual(s.time, TimeStamp(1237237351.064861))
        self.assertEqual(s.id, '10.0.0.1:40784')
        self.assertEqual(s.state, 'QueryResponse')
        self.assertEqual(s.body, """\
SELECT column1, column2 FROM table1 WHERE column3='foo'
""")


class TestSequence(unittest.TestCase):

    def testTime(self):
        seq = Sequence()
        seq.note(Event(10001.700, FakeJob, TestSource, 'QueryStart', 'SELECT'))
        seq.note(Event(10001.703, FakeJob, TestSource, 'QueryResult', 'SELECT'))
        seq.note(Event(10001.717, FakeJob, TestSource, 'Quit', 'Quit'))
        
        self.assertEqual(seq.count(), 3)
        self.assertEqual(seq.time(), TimeStamp(0.017))
    
    def testQuit(self):
        seq = Sequence()
        seq.note(Event(10001.700, FakeJob, TestSource, 'QueryStart', 'SELECT'))
        self.assert_(not seq.ended())
        seq.note(Event(10001.703, FakeJob, TestSource, 'QueryResult', 'SELECT'))
        self.assert_(not seq.ended())
        seq.note(Event(10001.717, FakeJob, TestSource, 'Quit', 'Quit'))
        self.assert_(seq.ended())
    
    def testGenerateEnd(self):
        seq = Sequence()
        seq.note(Event(10001.700, FakeJob, TestSource, 'QueryStart', 'SELECT'))
        seq.note(Event(10001.703, FakeJob, TestSource, 'QueryResult', 'SELECT'))
        self.assert_(not seq.ended())
        e = seq.generateEnd()
        self.assertEqual(e.time, TimeStamp(10001.703))
        self.assertEqual(e.id, FakeJob)
        self.assertEqual(e.state, 'Quit')
    
    def testTimeTo(self):
        seq = Sequence()
        e1 = Event(10001.700, FakeJob, TestSource, 'QueryStart', 'SELECT')
        e2 = Event(10001.717, FakeJob, TestSource, 'Quit', 'Quit')
        
        self.assert_(seq.timeto(e1) is None)
        
        seq.note(e1)
        self.assertEqual(seq.timeto(e1), TimeStamp(0))
        self.assertEqual(seq.timeto(e2), TimeStamp(0.017))
        


class SimpleCoalesce(CoalesceSequences):
    def __init__(self):
        CoalesceSequences.__init__(self)
        self.sequences = []
        
    def fullSequence(self, e):
        self.sequences.append(e)
        
class TestCoalesce(unittest.TestCase):

    def assertEvent(self, e, time, id, state, body=None):
        self.assertEqual(e.source, TestSource)
        if time is not None:
            if not isinstance(time, TimeStamp):
                time = TimeStamp(time)
            self.assertEqual(e.time, time)
        if id is not None:
            self.assertEqual(e.id, id)
        if state is not None:
            self.assertEqual(e.state, state)
        if body is not None:
            self.assertEqual(e.body, body)
            
    def testOne(self):
        c = CoalescedEvent()
        c.add(Event(10001.500, FakeJob, TestSource, 'QueryStart', 'SELECT "foo"'))
        c.add(Event(10001.600, FakeJob, TestSource, 'QueryStart', 'SELECT "bar"'))
        c.add(Event(10001.700, FakeJob, TestSource, 'Quit', 'Quit'))
        
        self.assertEvent(c, 10001.500, FakeJob, 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10001.600000:SELECT "bar"\n+++\n'
            '10001.700000:Quit\n+++\n')

    def testTwoSequential(self):
        l = []
        l.append(Event(10001.500, FakeJob, TestSource, 'QueryStart', 'SELECT "foo"'))
        l.append(Event(10001.600, FakeJob, TestSource, 'QueryStart', 'SELECT "bar"'))
        l.append(Event(10001.700, FakeJob, TestSource, 'Quit', 'Quit'))
        l.append(Event(10002.500, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "oof"'))
        l.append(Event(10002.600, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "rab"'))
        l.append(Event(10002.700, FakeJobs[0], TestSource, 'Quit', 'Quit'))
        
        sc = SimpleCoalesce()
        sc.replay(l)
        
        self.assertEqual(len(sc.sequences), 2)
        self.assertEvent(sc.sequences[0], 10001.500, FakeJob, 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10001.600000:SELECT "bar"\n+++\n'
            '10001.700000:Quit\n+++\n')
        self.assertEvent(sc.sequences[1], 10002.500, FakeJobs[0], 'Sequence',
            '10002.500000:SELECT "oof"\n+++\n'
            '10002.600000:SELECT "rab"\n+++\n'
            '10002.700000:Quit\n+++\n')
        
    def testTwoInterleaved(self):
        l = []
        l.append(Event(10001.500, FakeJob, TestSource, 'QueryStart', 'SELECT "foo"'))
        l.append(Event(10001.520, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "oof"'))
        l.append(Event(10001.600, FakeJob, TestSource, 'QueryStart', 'SELECT "bar"'))
        l.append(Event(10001.620, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "rab"'))
        l.append(Event(10001.700, FakeJob, TestSource, 'Quit', 'Quit'))
        l.append(Event(10001.720, FakeJobs[0], TestSource, 'Quit', 'Quit'))
        
        sc = SimpleCoalesce()
        sc.replay(l)
        
        self.assertEqual(len(sc.sequences), 2)
        self.assertEvent(sc.sequences[0], 10001.500, FakeJob, 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10001.600000:SELECT "bar"\n+++\n'
            '10001.700000:Quit\n+++\n')
        self.assertEvent(sc.sequences[1], 10001.520, FakeJobs[0], 'Sequence',
            '10001.520000:SELECT "oof"\n+++\n'
            '10001.620000:SELECT "rab"\n+++\n'
            '10001.720000:Quit\n+++\n')
        
    def testTwoNested(self):
        l = []
        l.append(Event(10001.500, FakeJob, TestSource, 'QueryStart', 'SELECT "foo"'))
        l.append(Event(10002.500, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "oof"'))
        l.append(Event(10002.600, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "rab"'))
        l.append(Event(10002.700, FakeJobs[0], TestSource, 'Quit', 'Quit'))
        l.append(Event(10003.600, FakeJob, TestSource, 'QueryStart', 'SELECT "bar"'))
        l.append(Event(10003.700, FakeJob, TestSource, 'Quit', 'Quit'))
        
        sc = SimpleCoalesce()
        sc.replay(l)
        
        self.assertEqual(len(sc.sequences), 2)
        self.assertEvent(sc.sequences[0], 10001.500, FakeJob, 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10003.600000:SELECT "bar"\n+++\n'
            '10003.700000:Quit\n+++\n')
        self.assertEvent(sc.sequences[1], 10002.500, FakeJobs[0], 'Sequence',
            '10002.500000:SELECT "oof"\n+++\n'
            '10002.600000:SELECT "rab"\n+++\n'
            '10002.700000:Quit\n+++\n')
        
    def testManyNested(self):
        l = []
        l.append(Event(10001.500, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "one"'))
        l.append(Event(10002.500, FakeJobs[1], TestSource, 'QueryStart', 'SELECT "two"'))
        l.append(Event(10002.700, FakeJobs[1], TestSource, 'Quit', 'Quit'))
        l.append(Event(10003.500, FakeJobs[2], TestSource, 'QueryStart', 'SELECT "three"'))
        l.append(Event(10003.700, FakeJobs[2], TestSource, 'Quit', 'Quit'))
        l.append(Event(10004.500, FakeJobs[3], TestSource, 'QueryStart', 'SELECT "four"'))
        l.append(Event(10004.700, FakeJobs[3], TestSource, 'Quit', 'Quit'))
        l.append(Event(10005.500, FakeJobs[4], TestSource, 'QueryStart', 'SELECT "five"'))
        l.append(Event(10005.700, FakeJobs[0], TestSource, 'Quit', 'Quit'))
        l.append(Event(10006.500, FakeJobs[5], TestSource, 'QueryStart', 'SELECT "six"'))
        l.append(Event(10006.700, FakeJobs[5], TestSource, 'Quit', 'Quit'))
        l.append(Event(10007.500, FakeJobs[6], TestSource, 'QueryStart', 'SELECT "seven"'))
        l.append(Event(10007.700, FakeJobs[6], TestSource, 'Quit', 'Quit'))
        l.append(Event(10008.500, FakeJobs[7], TestSource, 'QueryStart', 'SELECT "eight"'))
        l.append(Event(10008.700, FakeJobs[7], TestSource, 'Quit', 'Quit'))
        l.append(Event(10009.700, FakeJobs[4], TestSource, 'Quit', 'Quit'))

        sc = SimpleCoalesce()
        sc.replay(l)

        self.assertEqual(len(sc.sequences), 8)
        self.assertEqual(sc.sequences[0].id, FakeJobs[0])
        self.assertEqual(sc.sequences[1].id, FakeJobs[1])
        self.assertEqual(sc.sequences[2].id, FakeJobs[2])
        self.assertEqual(sc.sequences[3].id, FakeJobs[3])
        self.assertEqual(sc.sequences[4].id, FakeJobs[4])
        self.assertEqual(sc.sequences[5].id, FakeJobs[5])
        self.assertEqual(sc.sequences[6].id, FakeJobs[6])
        self.assertEqual(sc.sequences[7].id, FakeJobs[7])
    
    def testMissingEnd(self):
        l = []
        l.append(Event(10001.500, FakeJobs[0], TestSource, 'QueryStart', 'SELECT "one"'))
        l.append(Event(10002.500, FakeJobs[1], TestSource, 'QueryStart', 'SELECT "two"'))
        l.append(Event(10002.700, FakeJobs[1], TestSource, 'Quit', 'Quit'))
        l.append(Event(10003.500, FakeJobs[2], TestSource, 'QueryStart', 'SELECT "three"'))
    
        sc = SimpleCoalesce()
        sc.replay(l)
        self.assertEqual(len(sc.sequences), 3)
        self.assertEqual(sc.sequences[0].id, FakeJobs[0])
        self.assertEqual(sc.sequences[1].id, FakeJobs[1])
        self.assertEqual(sc.sequences[2].id, FakeJobs[2])

        es = sc.sequences[0].events()
        self.assertEqual(len(es), 2)
        self.assertEvent(es[0], 10001.500, FakeJobs[0], Event.Query, 'SELECT "one"')
        self.assertEvent(es[1], 10001.500, FakeJobs[0], Event.End)
        es = sc.sequences[1].events()
        self.assertEqual(len(es), 2)
        self.assertEvent(es[0], 10002.500, FakeJobs[1], Event.Query, 'SELECT "two"')
        self.assertEvent(es[1], 10002.700, FakeJobs[1], Event.End)
        es = sc.sequences[2].events()
        self.assertEqual(len(es), 2)
        self.assertEvent(es[0], 10003.500, FakeJobs[2], Event.Query, 'SELECT "three"')
        self.assertEvent(es[1], 10003.500, FakeJobs[2], Event.End)

    def testSplitApart(self):
        c = CoalescedEvent()
        c.add(Event(10001.500, FakeJob, TestSource, 'QueryStart', 'SELECT "foo"'))
        c.add(Event(10001.600, FakeJob, TestSource, 'QueryStart', 'SELECT "bar"\n'))
        c.add(Event(10001.700, FakeJob, TestSource, 'QueryStart', '\nSELECT "baz"'))
        c.add(Event(10001.800, FakeJob, TestSource, 'Quit', 'Quit'))
        
        e = parse_stanza(StringIO(str(c)))
        self.assertEqual(e.id, FakeJob)
        self.assertEqual(e.state, CoalescedEvent.Sequence)

        es = e.events();
        self.assertEqual(len(es), 4)
        
        self.assertEvent(es[0], 10001.500, FakeJob, Event.Query, 'SELECT "foo"')
        self.assertEvent(es[1], 10001.600, FakeJob, Event.Query, 'SELECT "bar"\n')
        self.assertEvent(es[2], 10001.700, FakeJob, Event.Query, '\nSELECT "baz"')
        self.assertEvent(es[3], 10001.800, FakeJob, Event.End)

if __name__ == '__main__':
    unittest.main()

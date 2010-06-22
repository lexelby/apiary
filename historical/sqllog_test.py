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

from sqllog import *
from timestamp import TimeStamp
    

class TestEvent(unittest.TestCase):

    def testEventStr(self):
        e = Event(123456.789, '888', 'sim', 'QueryStart', 'SELECT * FROM foo')
        self.assertEqual(str(e), """\
123456.789000\t888\tsim\tQueryStart
SELECT * FROM foo
**************************************
""")

    def testOrdering(self):
        e1 = Event(10001.700, '999', 'sim', 'QueryStart', 'SELECT')
        e2 = Event(10001.717, '999', 'sim', 'QueryResponse', 'SELECT')
        e3 = Event(10001.717, '999', 'sim', 'Quit', 'Quit')
        e4 = Event(10001.729, '878', 'sim', 'QueryStart', 'SELECT')
        
        self.assert_(e1 < e2)
        self.assert_(e1 < e3)
        self.assert_(e1 < e4)
        self.assert_(e2 < e3)
        self.assert_(e2 < e4)
        self.assert_(e3 < e4)


class TestParsing(unittest.TestCase):

    def testSimpleStanza(self):
        f = StringIO("""\
1237237351.064861\t10.2.231.65:40784\tsim\tQueryResponse
SELECT owner_id, is_owner_group FROM parcel WHERE parcel_id='9d50d6eb-a623-2a30-a8e0-840189fabff7'
**************************************
1237237351.065393\t10.6.6.97:39706\tsim\tQueryStart
SELECT u.username, uln.name, u.group_id  FROM user u, user_last_name uln  WHERE u.agent_id = '8c7d410a-ec70-430a-a3ef-1b44afe94cc2'  AND uln.last_name_id = u.last_name_id
**************************************
""")
        s = parse_stanza(f)
        self.assert_(s is not None)
        self.assertEqual(s.time, TimeStamp(1237237351.064861))
        self.assertEqual(s.id, '10.2.231.65:40784')
        self.assertEqual(s.state, 'QueryResponse')
        self.assertEqual(s.body, """\
SELECT owner_id, is_owner_group FROM parcel WHERE parcel_id='9d50d6eb-a623-2a30-a8e0-840189fabff7'
""")

    def testEmptyStanza(self):
        f = StringIO('')
        s = parse_stanza(f)
        self.assert_(s is None)
    
    def testMissingStanzaEnd(self):
        f = StringIO("""\
1237237351.064861\t10.2.231.65:40784\tsim\tQueryResponse
SELECT owner_id, is_owner_group FROM parcel WHERE parcel_id='9d50d6eb-a623-2a30-a8e0-840189fabff7'
""")
        s = parse_stanza(f)
        self.assert_(s is not None)
        self.assertEqual(s.time, TimeStamp(1237237351.064861))
        self.assertEqual(s.id, '10.2.231.65:40784')
        self.assertEqual(s.state, 'QueryResponse')
        self.assertEqual(s.body, """\
SELECT owner_id, is_owner_group FROM parcel WHERE parcel_id='9d50d6eb-a623-2a30-a8e0-840189fabff7'
""")

    def testJunkLeadInStanza(self):
        f = StringIO("""\
SELECT u.username, uln.name, u.group_id  FROM user u, user_last_name uln  WHERE u.agent_id = '8c7d410a-ec70-430a-a3ef-1b44afe94cc2'  AND uln.last_name_id = u.last_name_id
**************************************
1237237351.064861\t10.2.231.65:40784\tsim\tQueryResponse
SELECT owner_id, is_owner_group FROM parcel WHERE parcel_id='9d50d6eb-a623-2a30-a8e0-840189fabff7'
**************************************
""")
        s = parse_stanza(f)
        self.assert_(s is not None)
        self.assertEqual(s.time, TimeStamp(1237237351.064861))
        self.assertEqual(s.id, '10.2.231.65:40784')
        self.assertEqual(s.state, 'QueryResponse')
        self.assertEqual(s.body, """\
SELECT owner_id, is_owner_group FROM parcel WHERE parcel_id='9d50d6eb-a623-2a30-a8e0-840189fabff7'
""")


class TestSequence(unittest.TestCase):

    def testTime(self):
        seq = Sequence()
        seq.note(Event(10001.700, '999', 'sim', 'QueryStart', 'SELECT'))
        seq.note(Event(10001.703, '999', 'sim', 'QueryResult', 'SELECT'))
        seq.note(Event(10001.717, '999', 'sim', 'Quit', 'Quit'))
        
        self.assertEqual(seq.count(), 3)
        self.assertEqual(seq.time(), TimeStamp(0.017))
    
    def testQuit(self):
        seq = Sequence()
        seq.note(Event(10001.700, '999', 'sim', 'QueryStart', 'SELECT'))
        self.assert_(not seq.ended())
        seq.note(Event(10001.703, '999', 'sim', 'QueryResult', 'SELECT'))
        self.assert_(not seq.ended())
        seq.note(Event(10001.717, '999', 'sim', 'Quit', 'Quit'))
        self.assert_(seq.ended())
    
    def testGenerateEnd(self):
        seq = Sequence()
        seq.note(Event(10001.700, '999', 'sim', 'QueryStart', 'SELECT'))
        seq.note(Event(10001.703, '999', 'sim', 'QueryResult', 'SELECT'))
        self.assert_(not seq.ended())
        e = seq.generateEnd()
        self.assertEqual(e.time, TimeStamp(10001.703))
        self.assertEqual(e.id, '999')
        self.assertEqual(e.state, 'Quit')
    
    def testTimeTo(self):
        seq = Sequence()
        e1 = Event(10001.700, '999', 'sim', 'QueryStart', 'SELECT')
        e2 = Event(10001.717, '999', 'sim', 'Quit', 'Quit')
        
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
        c.add(Event(10001.500, '999', 'sim', 'QueryStart', 'SELECT "foo"'))
        c.add(Event(10001.600, '999', 'sim', 'QueryStart', 'SELECT "bar"'))
        c.add(Event(10001.700, '999', 'sim', 'Quit', 'Quit'))
        
        self.assertEvent(c, 10001.500, '999', 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10001.600000:SELECT "bar"\n+++\n'
            '10001.700000:Quit\n+++\n')

    def testTwoSequential(self):
        l = []
        l.append(Event(10001.500, '999', 'sim', 'QueryStart', 'SELECT "foo"'))
        l.append(Event(10001.600, '999', 'sim', 'QueryStart', 'SELECT "bar"'))
        l.append(Event(10001.700, '999', 'sim', 'Quit', 'Quit'))
        l.append(Event(10002.500, '888', 'sim', 'QueryStart', 'SELECT "oof"'))
        l.append(Event(10002.600, '888', 'sim', 'QueryStart', 'SELECT "rab"'))
        l.append(Event(10002.700, '888', 'sim', 'Quit', 'Quit'))
        
        sc = SimpleCoalesce()
        sc.replay(l)
        
        self.assertEqual(len(sc.sequences), 2)
        self.assertEvent(sc.sequences[0], 10001.500, '999', 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10001.600000:SELECT "bar"\n+++\n'
            '10001.700000:Quit\n+++\n')
        self.assertEvent(sc.sequences[1], 10002.500, '888', 'Sequence',
            '10002.500000:SELECT "oof"\n+++\n'
            '10002.600000:SELECT "rab"\n+++\n'
            '10002.700000:Quit\n+++\n')
        
    def testTwoInterleaved(self):
        l = []
        l.append(Event(10001.500, '999', 'sim', 'QueryStart', 'SELECT "foo"'))
        l.append(Event(10001.520, '888', 'sim', 'QueryStart', 'SELECT "oof"'))
        l.append(Event(10001.600, '999', 'sim', 'QueryStart', 'SELECT "bar"'))
        l.append(Event(10001.620, '888', 'sim', 'QueryStart', 'SELECT "rab"'))
        l.append(Event(10001.700, '999', 'sim', 'Quit', 'Quit'))
        l.append(Event(10001.720, '888', 'sim', 'Quit', 'Quit'))
        
        sc = SimpleCoalesce()
        sc.replay(l)
        
        self.assertEqual(len(sc.sequences), 2)
        self.assertEvent(sc.sequences[0], 10001.500, '999', 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10001.600000:SELECT "bar"\n+++\n'
            '10001.700000:Quit\n+++\n')
        self.assertEvent(sc.sequences[1], 10001.520, '888', 'Sequence',
            '10001.520000:SELECT "oof"\n+++\n'
            '10001.620000:SELECT "rab"\n+++\n'
            '10001.720000:Quit\n+++\n')
        
    def testTwoNested(self):
        l = []
        l.append(Event(10001.500, '999', 'sim', 'QueryStart', 'SELECT "foo"'))
        l.append(Event(10002.500, '888', 'sim', 'QueryStart', 'SELECT "oof"'))
        l.append(Event(10002.600, '888', 'sim', 'QueryStart', 'SELECT "rab"'))
        l.append(Event(10002.700, '888', 'sim', 'Quit', 'Quit'))
        l.append(Event(10003.600, '999', 'sim', 'QueryStart', 'SELECT "bar"'))
        l.append(Event(10003.700, '999', 'sim', 'Quit', 'Quit'))
        
        sc = SimpleCoalesce()
        sc.replay(l)
        
        self.assertEqual(len(sc.sequences), 2)
        self.assertEvent(sc.sequences[0], 10001.500, '999', 'Sequence',
            '10001.500000:SELECT "foo"\n+++\n'
            '10003.600000:SELECT "bar"\n+++\n'
            '10003.700000:Quit\n+++\n')
        self.assertEvent(sc.sequences[1], 10002.500, '888', 'Sequence',
            '10002.500000:SELECT "oof"\n+++\n'
            '10002.600000:SELECT "rab"\n+++\n'
            '10002.700000:Quit\n+++\n')
        
    def testManyNested(self):
        l = []
        l.append(Event(10001.500, '111', 'sim', 'QueryStart', 'SELECT "one"'))
        l.append(Event(10002.500, '222', 'sim', 'QueryStart', 'SELECT "two"'))
        l.append(Event(10002.700, '222', 'sim', 'Quit', 'Quit'))
        l.append(Event(10003.500, '333', 'sim', 'QueryStart', 'SELECT "three"'))
        l.append(Event(10003.700, '333', 'sim', 'Quit', 'Quit'))
        l.append(Event(10004.500, '444', 'sim', 'QueryStart', 'SELECT "four"'))
        l.append(Event(10004.700, '444', 'sim', 'Quit', 'Quit'))
        l.append(Event(10005.500, '555', 'sim', 'QueryStart', 'SELECT "five"'))
        l.append(Event(10005.700, '111', 'sim', 'Quit', 'Quit'))
        l.append(Event(10006.500, '666', 'sim', 'QueryStart', 'SELECT "six"'))
        l.append(Event(10006.700, '666', 'sim', 'Quit', 'Quit'))
        l.append(Event(10007.500, '777', 'sim', 'QueryStart', 'SELECT "seven"'))
        l.append(Event(10007.700, '777', 'sim', 'Quit', 'Quit'))
        l.append(Event(10008.500, '888', 'sim', 'QueryStart', 'SELECT "eight"'))
        l.append(Event(10008.700, '888', 'sim', 'Quit', 'Quit'))
        l.append(Event(10009.700, '555', 'sim', 'Quit', 'Quit'))

        sc = SimpleCoalesce()
        sc.replay(l)

        self.assertEqual(len(sc.sequences), 8)
        self.assertEqual(sc.sequences[0].id, '111')
        self.assertEqual(sc.sequences[1].id, '222')
        self.assertEqual(sc.sequences[2].id, '333')
        self.assertEqual(sc.sequences[3].id, '444')
        self.assertEqual(sc.sequences[4].id, '555')
        self.assertEqual(sc.sequences[5].id, '666')
        self.assertEqual(sc.sequences[6].id, '777')
        self.assertEqual(sc.sequences[7].id, '888')
    
    def testMissingEnd(self):
        l = []
        l.append(Event(10001.500, '111', 'sim', 'QueryStart', 'SELECT "one"'))
        l.append(Event(10002.500, '222', 'sim', 'QueryStart', 'SELECT "two"'))
        l.append(Event(10002.700, '222', 'sim', 'Quit', 'Quit'))
        l.append(Event(10003.500, '333', 'sim', 'QueryStart', 'SELECT "three"'))
    
        sc = SimpleCoalesce()
        sc.replay(l)
        self.assertEqual(len(sc.sequences), 3)
        self.assertEqual(sc.sequences[0].id, '111')
        self.assertEqual(sc.sequences[1].id, '222')
        self.assertEqual(sc.sequences[2].id, '333')

        es = sc.sequences[0].events()
        self.assertEqual(len(es), 2)
        self.assertEvent(es[0], 10001.500, '111', Event.Query, 'SELECT "one"')
        self.assertEvent(es[1], 10001.500, '111', Event.End)
        es = sc.sequences[1].events()
        self.assertEqual(len(es), 2)
        self.assertEvent(es[0], 10002.500, '222', Event.Query, 'SELECT "two"')
        self.assertEvent(es[1], 10002.700, '222', Event.End)
        es = sc.sequences[2].events()
        self.assertEqual(len(es), 2)
        self.assertEvent(es[0], 10003.500, '333', Event.Query, 'SELECT "three"')
        self.assertEvent(es[1], 10003.500, '333', Event.End)

    def testSplitApart(self):
        c = CoalescedEvent()
        c.add(Event(10001.500, '999', 'sim', 'QueryStart', 'SELECT "foo"'))
        c.add(Event(10001.600, '999', 'sim', 'QueryStart', 'SELECT "bar"\n'))
        c.add(Event(10001.700, '999', 'sim', 'QueryStart', '\nSELECT "baz"'))
        c.add(Event(10001.800, '999', 'sim', 'Quit', 'Quit'))
        
        e = parse_stanza(StringIO(str(c)))
        self.assertEqual(e.id, '999')
        self.assertEqual(e.state, CoalescedEvent.Sequence)

        es = e.events();
        self.assertEqual(len(es), 4)
        
        self.assertEvent(es[0], 10001.500, '999', Event.Query, 'SELECT "foo"')
        self.assertEvent(es[1], 10001.600, '999', Event.Query, 'SELECT "bar"\n')
        self.assertEvent(es[2], 10001.700, '999', Event.Query, '\nSELECT "baz"')
        self.assertEvent(es[3], 10001.800, '999', Event.End)

if __name__ == '__main__':
    unittest.main()

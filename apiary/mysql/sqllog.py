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

import heapq
import re
import sys

from apiary.tools.debug import *
from apiary.tools import mergetools
from apiary.tools import timestamp

__all__ = [
        'Event',
        'CoalescedEvent',
        'Sequence',
        'parse_stanza',
        'EventReader',
        'input_events',
        'FollowSequences',
        'CoalesceSequences'
    ]

headerRE = re.compile(r'^(\d+\.\d+)\t([\d.:]+)\t(\S+)\t(\w+)$')
breakRE = re.compile(r'^\*{3,}$')
commentRE = re.compile(r'^\-{2,}.*$')


class Event(object):
    # state values
    Query = 'QueryStart'
    Response = 'QueryResponse'
    End = 'Quit'
    
    #@traced_method
    def __init__(self, time, id, source, state, body):
        self.time = time
        if type(time) is not timestamp.TimeStamp:
            self.time = timestamp.TimeStamp(time)
        self.id = id
        self.source = source
        self.state = state
        self.body = body
    
    def __cmp__(self, other):
        c = cmp(self.time, other.time)
        if c == 0:
            if self.state == "Quit" and other.state != "Quit":
                return 1
            if self.state != "Quit" and other.state == "Quit":
                return -1
        return c
        
    def __str__(self):
        return ("%s\t%s\t%s\t%s\n%s\n**************************************\n"
            % (self.time, self.id, self.source, self.state, self.body))
    
    def events(self):
        es = []
        for event in self.body.split("\n+++\n"):
            parts = event.split(":", 1)
            if len(parts) == 2:
                (time, body) = parts
                status = Event.Query
                if body == "Quit":
                    status = Event.End
                es.append(Event(time, self.id, self.source, status, body))
        return es

class CoalescedEvent(Event):
    Sequence = 'Sequence'
    
    def __init__(self, shelf_life=None, max_life=None):
        self.time = None
        self.id = None
        self.source = None
        self.state = CoalescedEvent.Sequence
        self.body = ""
        self.ended = False
        self.lasttime = None
        self.staletime = None
        self.maxtime = None
        self.shelf_life = None
        if shelf_life:
            self.shelf_life = timestamp.TimeStamp(shelf_life)
        self.max_life = None
        if max_life:
            self.max_life = timestamp.TimeStamp(max_life)
    
    def add(self, event):
        if self.time is None:
            self.time = event.time
            if self.max_life:
                self.maxtime = self.time + self.max_life
            self.id = event.id
            self.source = event.source
        self.lasttime = event.time
        if self.shelf_life:
            self.staletime = self.lasttime + self.shelf_life
        if event.state == Event.End:
            self.body += "%s:Quit\n+++\n" % (event.time)
            self.ended = True
        elif event.state == Event.Query:
            self.body += "%s:%s\n+++\n" % (event.time, event.body) 
        # Ignore Event.QueryResponse, because we only care about queries sent.
    
    def endIfNeeded(self):
        if not self.ended:
            self.add(Event(self.lasttime, self.id, self.source,
                            Event.End, 'Quit'))


#@traced_func
def parse_stanza(input):
    match = None
    while not match:
        line = input.readline()
        if line == '':  # empty string means EOF
            return None
        if commentRE.match(line): # catch comments before the headers
            line = input.readline() # Skip the line
        match = headerRE.match(line)
    (time, id, source, state) = match.groups()
    
    body = ''
    while True:
        line = input.readline()
        if commentRE.match(line):
            line = input.readline() # Skip the line
        if line == '':
            break
        if breakRE.match(line):
            break
        body += line
    
    return Event(float(time), id, source, state, body)
    
class Sequence(object):
    def __init__(self):
        self._count = 0
        self._time_start = None
        self._last_event = None
    
    def note(self, event):
        self._count += 1
        if self._time_start is None:
            self._time_start = event.time
        self._last_event = event
    
    def count(self):
        return self._count
    
    def time(self):
        return self._last_event.time - self._time_start
    
    def timeto(self, event):
        if self._last_event is None:
            return None
        return event.time - self._last_event.time
        
    def ended(self):
        return self._last_event.state == Event.End
        
    def generateEnd(self, t=None):
        e = self._last_event
        if t is None:
            t = e.time
        return Event(t, e.id, e.source, Event.End, "")


class EventReader(object):
    def __init__(self, input):
        self._input = input
    
    def __iter__(self):
        while True:
            s = parse_stanza(self._input)
            if s is None:
                return
            if s.state == CoalescedEvent.Sequence:
                for t in s.events():
                    yield t
            else:
                yield s

def input_spec_to_file(spec):
    if spec == '-':
        return sys.stdin
    return file(spec)

#@traced_func
def input_events(specs):
    if len(specs) == 0:
        return iter(EventReader(sys.stdin))
    evs = map(EventReader, map(input_spec_to_file, specs))
    return mergetools.imerge(*evs)


class FollowSequences(object):
    def replay(self, events):
        connections = { }
        lastt = None;
        for e in events:
            id = e.id
            lastt = e.time
            if id not in connections:
                s = connections[id] = Sequence()
                self.addingSequence(s, e)
            else:
                s = connections[id]
            self.notingEvent(s, e)
            s.note(e)
            if s.ended():
                self.removingSequence(s, e)
                del connections[id]
            if False:
                expired = []
                for (id,s) in connections.iteritems():
                    w = s.timeto(e)
                    if w and float(w) > 60.0:
                        expired.append((id,s))
                for (id,s) in expired:
                    f = s.generateEnd(e.time)
                    self.forcedEnd(s, f)
                    self.removingSequence(s, f)
                    del connections[id]
        for s in connections.itervalues():
            f = s.generateEnd(lastt)
            self.forcedEnd(s, f)
            self.removingSequence(s, f)
            
    def addingSequence(self, s, e):
        pass
    
    def notingEvent(self, s, e):
        pass
    
    def forcedEnd(self, s, e):
        pass
    
    def removingSequence(self, s, e):
        pass


class CoalesceSequences(object):
    def __init__(self):
        self.connections = { }
        self.bytime = [ ]
        self.starttime = None
        self.lasttime = None
        
    def heartbeat(self, n):
        sys.stderr.write("front of the queue = %s" % self.bytime[0].id)
        sys.stderr.write("%s: %d events... (%d connections, %d waiting)\n"
            % (str(self.lasttime - self.starttime), n, len(self.connections), len(self.bytime)))
        n = 0
        i = 0
        l = len(self.bytime)
        s = ""
        while n < 5 and i < l:
            en = 0
            while i < l and self.bytime[i].ended:
                en += 1
                i += 1
            if en > 0:
                s += " : --%d--" % en
            else:
                n += 1
                s += " : %s(%s)" % (self.bytime[i].id, str(self.lasttime - self.bytime[i].lasttime))
                i += 1
        sys.stderr.write("                          ")
        sys.stderr.write(s)
        sys.stderr.write("\n")

    def age_out(self, c):
        if c.staletime and self.lasttime >= c.staletime:
            sys.stderr.write("                           expiring %s, stale\n" % c.id)
        elif c.maxtime and self.lasttime >= c.maxtime:
            sys.stderr.write("                           expiring %s, maxed out\n" % c.id)
        else:
            return False
        c.endIfNeeded()
        del self.connections[c.id]
        return True
        
    def flush_completed(self):
        bytime = self.bytime
        
        while bytime:
            c = bytime[0]
            if not c.ended:
                if not self.age_out(c):
                    return
            heapq.heappop(bytime)
            self.fullSequence(c)


    def replay(self, events):
        n = 0;
        connections = self.connections
        bytime = self.bytime
        for e in events:
            id = e.id
            self.lasttime = e.time
            if self.starttime is None:
                self.starttime = self.lasttime

            n += 1
            if n % 10000 == 0:
                self.heartbeat(n)
            
            if id in connections:
                c = connections[id]
                self.age_out(c)

            if id not in connections:
                c = connections[id] = CoalescedEvent(30.0, 180.0)
                c.add(e)
                heapq.heappush(bytime, c)
            else:
                c.add(e)

            if e.state == Event.End:
                del connections[id]

            self.flush_completed()

        for d in connections.itervalues():
            d.endIfNeeded()
        self.flush_completed()
                    
    def fullSequence(self, e):
        pass


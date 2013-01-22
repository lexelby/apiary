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
import time
import cPickle

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

headerRE = re.compile(r'^(?P<time>\d+\.\d+)\t(?P<id>[\d.:]+)\t?(?P<source>\S*)\t(?P<state>\w+)$')
breakRE = re.compile(r'^\*{3,}$')
commentRE = re.compile(r'^\-{2,}.*$')
timeRE = re.compile(r'^# Time: (\d+ [\d\w:.]+)$')
clientRE = re.compile(r'^# Client: ([\d.:]+)$')
threadRE = re.compile(r'# Thread_id: (\d+)$')
admin_commandRE = re.compile(r'^# administrator command: (\w+);$')
query_log_commentRE = re.compile(r'^#')

class EventFile(object):
    def __init__(self, name):
        if name == "-":
            self._file = sys.stdin
        else:
            self._file = open(name, 'r')
            
        self._lines = []
    
    def readline(self):
        if self._lines:
            return self._lines.pop()
        else:
            return self._file.readline()
    
    def unreadline(self, line):
        self._lines.append(line)

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
    
    # This function is a bit gnarly because it can handle either the output of
    # mk-query-digest or the output of mysql_watcher/mysql_logger.
    #
    # mk-query-digest parsing code borrowed from Dante Linden.
    
    seconds = 0
    id = ''
    source = ''
    state = Event.Query
    admin_command = ''
    line = ''
    body = ''
    match = None
    while not match and not query_log_commentRE.match(line):
        line = input.readline()
        if line == '':  # empty string means EOF
            return None
        if commentRE.match(line): # catch comments before the headers
            line = input.readline() # Skip the line
        # parse the header in case it's a seq log
        match = headerRE.match(line)
    
    if match:
        seconds = match.group('time')
        id = match.group('id')
        source = match.group('source') or ''
        state = match.group('state')
        
        line = input.readline()

    # if processing a mk-query-digest query log, extract info from comments
    while query_log_commentRE.match(line):
        if timeRE.match(line):
            # if seconds, then you've hit another query in a digest log because
            # the previous query had no body (i.e., the comments from one query
            # border the comments for the next query)
            if seconds:
                # seek backward so we can process this timestamp as part of the
                # next stanza
                input.unreadline(line)
                return Event(float(seconds), id, source, state, body)

            timestamp = timeRE.match(line).groups()[0]
            # convert timestamp into seconds since epoch:
            # strip off subseconds, convert remainder to seconds, 
            # append subseconds
            parts = timestamp.split('.')
            if len(parts) == 2:
                date_time, subseconds = timestamp.split('.')
            else:
                date_time = timestamp
                subseconds = 0
            seconds = str(int(time.mktime(time.strptime(date_time, "%y%m%d %H:%M:%S")))) + ".%s" % subseconds
        if clientRE.match(line):
            id = clientRE.match(line).groups()[0]
        if threadRE.match(line):
            id += ':%s' % threadRE.match(line).groups(0)
        if admin_commandRE.match(line):
            admin_command = admin_commandRE.match(line).groups()[0]
            if admin_command == "Quit":
                state = Event.End
    
        line = input.readline()
        if line == '':
            return None

        
    # we should be to the body of the stanza now 
    while True:
        while commentRE.match(line):
            line = input.readline() # Skip the line
        if line == '': # EOF
            break
        if breakRE.match(line):
            break
        if query_log_commentRE.match(line):
            break
        body += line
        line = input.readline()

    # any admin commands follow the body
    if admin_commandRE.match(line):
        admin_command = admin_commandRE.match(line).groups()[0]
        if admin_command == "Quit":
            state = Event.End
    else:
        # the last line we read was a comment.  seek backwards
        # so that we see it the next time we read from input
        input.unreadline(line)
    #print "seconds=%s, id=%s, body=%s" % (seconds, id, body)
    return Event(float(seconds), id, source, state, body)
    
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


class RawEventReader(object):
    def __init__(self, input):
        self._input = input
    
    def __iter__(self):
        while True:
            s = parse_stanza(self._input)
            if s is None:
                return
            yield s

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
                
#@traced_func
def input_events(specs):
    if len(specs) == 0:
        specs = ['-']
    evs = [EventReader(EventFile(spec)) for spec in specs]
    return mergetools.imerge(*evs)

def split_events(specs, dest_prefix, num_splits):
    if len(specs) == 0:
        specs = ['-']

    dests = [open(dest_prefix + str(n), 'w') for n in xrange(num_splits)]
    
    evs = [EventReader(EventFile(spec)) for spec in specs]
    merged = mergetools.imerge(*evs)
    
    start_time = time.time()
    
    for num, event in enumerate(merged):
        if num % 10000 == 0:
            elapsed = time.time() - start_time
            print "split %d events in %s seconds (%.2f events/sec)..." % (num, elapsed, float(num) / float(elapsed))
        
        dests[num % num_splits].write(str(event))

def pickle_events(specs, dest):
    if len(specs) == 0:
        specs = ['-']
    
    if not isinstance(dest, file):
        dest = open(dest, 'w')
    
    evs = [EventReader(EventFile(spec)) for spec in specs]
    merged = mergetools.imerge(*evs)
    
    start_time = time.time()
    
    for num, event in enumerate(merged):
        if num % 10000 == 0:
            elapsed = time.time() - start_time
            print "pickled %d events in %s seconds (%.2f events/sec)..." % (num, elapsed, float(num) / float(elapsed))
        
        if event.state == CoalescedEvent.Sequence:
            for subevent in event.events():
                cPickle.dump(subevent, file=dest)
        else:
            cPickle.dump(event, file=dest)


def input_pickled_events(specs):
    for spec in specs:
        sequence_file = open(spec)
        
        while True:
            try:
                event = cPickle.load(sequence_file)
                if event.state == CoalescedEvent.Sequence:
                    for subevent in event.events():
                        yield subevent
                else:
                    yield event
            except EOFError:
                break
        
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
        if self.bytime:
            sys.stderr.write("front of the queue = %s" % self.bytime[0].id)
        sys.stderr.write("%s: %d events... (%d connections, %d waiting)\n"
            % (str(self.lasttime - self.starttime), n, len(self.connections), len(self.bytime)))
            
        # The stuff below summarizes the queue in this format:
        # : <item> : <item> : <item>
        # Where <item> is one of:
        #   connection id(time since last message seen)
        #   -- n -- where n is a number of ended connections just waiting to be printed
        #
        # Up to the first 5 connections found will be printed, along with the gaps of ended connections
        # between them.
        
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
        """Print query sequences that have completed.
        
        Query sequences are always printed in order of the time the
        sequence started.  Sequence endings are sometimes not present
        in the event stream, in which case we must wait until the
        sequence times out before printing it out.  No sequences after
        the "stuck" sequence will be printed until it times out.
        """
        
        bytime = self.bytime
        
        while bytime:
            c = bytime[0]
            if not c.ended:
                if not self.age_out(c):
                    return
            heapq.heappop(bytime)
            self.fullSequence(c)
            
    def flush_all(self):
        """Flush all sequences.
        
        Sequences are flushed even if no end event has been seen and 
        they have not timed out yet.
        """
        
        while self.bytime:
            c = heapq.heappop(self.bytime)
            c.endIfNeeded()
            self.fullSequence(c)

    def replay(self, events):
        """Correlate an interleaved query stream into sequences of queries."""
        
        # n = number of events seen
        n = 0
        
        # s = number of sequences seen
        s = 0
        
        # self.connections tracks open connections for which we have not seen an end event.
        connections = self.connections
        
        # bytime contains all queries that have not yet been printed as 
        # sequences.  It is a min-heap ordered by time, so that the 
        # earliest event is always first in the list.
        bytime = self.bytime
        
        for e in events:
            id = e.id
            self.lasttime = e.time
            if self.starttime is None:
                self.starttime = self.lasttime

            n += 1
            if n % 10000 == 0:
                # Print stats every 10000 events.
                self.heartbeat(n)
            
            # If this connection is already in the lsit of open connections, 
            # see if it's stale or too old.  Sometimes the query stream doesn't
            # contain the End event for a given connection, so we need to time
            # it out.
            if id in connections:
                c = connections[id]
                self.age_out(c)

            # At this point, the connection may have been aged out and removed from
            # self.connections.  Otherwise, this could be the first message seen
            # for this connection.  In either case, make a new connection.
            if id not in connections:
                s += 1
                c = connections[id] = CoalescedEvent(300.0, 900.0)
                c.add(e)
                heapq.heappush(bytime, c)
            else:
                c.add(e)

            # Check if the connection is closing.  If so, remove it from 
            # self.connections, but don't print it out yet.  Events must
            # be printed in order.
            if e.state == Event.End:
                del connections[id]

            self.flush_completed()

        for d in connections.itervalues():
            d.endIfNeeded()
        self.flush_all()
        
        print >> sys.stderr, "%d events processed; %d sequences produced" % (n, s)
                    
    def fullSequence(self, e):
        raise NotImplemented("fullSequence() should be implemented by a child class")


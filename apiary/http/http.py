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

from optparse import OptionParser
import re
import time
import socket

import apiary
from apiary.tools.codec import Message
from apiary.tools.timer import Timer
from apiary.tools.stattools import StatValue
from apiary.tools.span import Span, SpanSequence, SlidingWindowSequence
from apiary.tools.debug import debug, traced_method
from apiary.tools.dummyfile import DummyFile


class HTTPWorkerBee(apiary.WorkerBee):
    _fake_results = ['HTTP/1.0 200 OK\r\nSome-Header: some value\r\n\r\n',
                     'HTTP/1.0 404 Not Found\r\nAnother-Header: another value\r\n\r\n']

    def __init__(self, options, arguments):
        apiary.WorkerBee.__init__(self, options, arguments)
        self._host = options.http_host
        if self._host == 'dummy':
            self._host = None
        self._port = options.http_port
        self._conn = None
        # _result is either None or (valid, details)
        # valid is a bool specifying whether the HTTP response was successfully parsed.
        # If valid is True, details contains the HTTP response.
        # If valid is False, details contains a generic error message.
        self._result = None
        self._timer = None
        self._worker_hostname = socket.getfqdn()
    
    #@traced_method
    def start(self):
        assert self._conn is None, 'Precondition violation: start called without ending previous session.'
        self._record_socket_errors(self._raw_start)
        
    def _raw_start(self):
        self._timer = Timer()
        if self._host:
            self._conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self._conn.connect((self._host, self._port))
            finally:
                # Record a 'connect' time regardless of exception since analysis code depends on its presence:
                self._timer.event('connect')
        
    #@traced_method
    def event(self, request):
        # Precondition:
        if self._result is not None:
            assert self._result[0] == False, 'Precondition violation: expected failure result; got: %r' % (self._result,)
            return # Connection failure.
        self._record_socket_errors(self._raw_event, request)
        # Postcondition:
        assert type(self._result) is not None, `self._result`
                
    def _raw_event(self, request, bufsize=2**14):
        reqlen = len(request)
        
        if self._host:
            assert self._conn, 'Precodingion violation: event called without starting a session.'
            self._timer.event('start-send')
            while request:
                written = self._conn.send(request)
                request = request[written:]
            self._timer.event('start-recv')
            inbuf = ''
            bytes = self._conn.recv(bufsize)
            i = -1
            while bytes and i == -1:
                inbuf += bytes
                i = inbuf.find('\r\n')
                bytes = self._conn.recv(bufsize)
            if i >= 0:
                response = inbuf[:i]
                # Read until the socket closes:
                resplen = len(inbuf)
                chunk = self._conn.recv(bufsize)
                while chunk:
                    resplen += len(chunk)
                    chunk = self._conn.recv(bufsize)
                self._set_result_from_http_response(inbuf[:i],
                                                    reqlen,
                                                    resplen)
                self._timer.event('parse-response')
            if self._result is None:
                self._timer.event('parse-response-fail')
                self._error_result('HTTP Response line not found: %r', inbuf[:256])
            self._timer.event('finish-recv')
        else:
            self._set_result_from_http_response(self._fake_results.pop(0))
            self._fake_results.append(self._result)
            
    #@traced_method
    def end(self):
        assert self._result is not None, 'Precondition violation: .end() precondition failed, no result.'
        
        if self._host and self._conn is not None:
            self._conn.close()
            self._timer.event('close')
            self._conn = None
        
        tdict = dict(self._timer.intervals)
        assert tdict.has_key('connect') and tdict.has_key('close'), 'Postcondition violation, missing timings: %r' % (tdict,)

        validresponse, details = self._result[:2]

        lengthinfo = {}
        if validresponse:
            reqlen, resplen = self._result[2:]
            lengthinfo['request_length'] = reqlen
            lengthinfo['response_length'] = resplen

        self._result = None

        msg = Message(details,
                      worker=self._worker_hostname,
                      valid_response=validresponse,
                      timings=self._timer.intervals,
                      **lengthinfo)
        return msg.encode_to_string()

    def _set_result_from_http_response(self, result, reqlen, resplen):
        m = self._HTTPStatusPattern.match(result)
        if m is None:
            self._result = (False, 'Failed to parse HTTP Response.', reqlen, resplen)
        else:
            self._result = (True, result, reqlen, resplen)
            
    def _error_result(self, tmpl, *args):
        self._result = (False, tmpl % args)

    def _record_socket_errors(self, f, *args):
        try:
            return f(*args)
        except socket.error, e:
            self._timer.event('close')
            self._conn = None
            # _result may already be set, for example, if we've parsed a
            # response and we are in the middle of reading the headers/body.
            if self._result is None:
                self._error_result('socket.error: %r', e.args)

    _HTTPStatusPattern = re.compile(r'(HTTP/\d\.\d) (\d{3}) (.*?)$', re.MULTILINE)


class HTTPQueenBee(apiary.QueenBee):
    def __init__(self, options, arguments, updateperiod=5):
        apiary.QueenBee.__init__(self, options, arguments)
        try:
            [self._inpath] = arguments
        except ValueError, e:
            raise SystemExit('Usage error: HTTPQueenBee needs an events data file.')
        dumppath = options.http_dump
        if dumppath is None:
            self._dumpfile = DummyFile()
        else:
            self._dumpfile = open(dumppath, 'wb')
            
        self._updateperiod = updateperiod
        self._fp = None
        self._eventgen = None
        self._jobid = 0
        self._histogram = {} # { HTTPStatus -> absolute_frequency }
        self._timingstats = {} # { timingtag -> StatValue }
        self._rps = StatValue()
        self._cps = StatValue() # "concurrency-per-second"
        self._roundtrip = StatValue()
        self._slwin = SlidingWindowSequence(updateperiod+0.5)
        self._allspans = SpanSequence()
        self._tally_time = 0
        
    def next(self):
        if self._fp is None:
            assert self._eventgen is None, 'Invariant violation: _fp set, but _eventgen is None.'
            self._fp = open(self._inpath, 'rb')
            self._eventgen = Message.decode_many_from_file(self._fp)
        try:
            msg = self._eventgen.next().body
        except StopIteration:
            return False

        jobid = self._next_jobid()
        self.start(jobid)
        self.event(jobid, msg)
        self.end(jobid)
        return True
    
    def result(self, seq, msgenc):
        msg = Message.decode_from_string(msgenc)

        msg.headers['seq'] = seq
        msg.encode_to_file(self._dumpfile)

        self._update_histogram(msg)
        self._record_timing_stats(msg)
        if time.time() > self._tally_time:
            self.print_tally()

    def print_tally(self):
        totalcount = reduce(lambda a, b: a+b,
                            self._histogram.values())
        print
        print "       count - frequency - message"
        print "------------   ---------   ---------------------------------------"
        for k, v in sorted(self._histogram.items()):
            relfreq = 100.0 * float(v) / float(totalcount)
            print ("%12d -   %6.2f%% - %s" % (v, relfreq, k))

        print
        print "  timing event - stats"
        print "--------------   ---------------------------------------"
        for event, stat in sorted(self._timingstats.items()):
            print '%14s   %s' % (event, stat.format())
            
        print
        print "RPS, Concurrency, and Response Time"
        print "-----------------------------------"

        self._update_timing_stats()
        print '%14s   %s' % ('rps', self._rps.format())
        print '%14s   %s' % ('concurrency', self._cps.format())
        print '%14s   %s' % ('roundtrip', self._roundtrip.format())
 
        self._tally_time = time.time() + self._updateperiod
        
    def main(self):
        t = - time.time()
        c = - time.clock()
        apiary.QueenBee.main(self)
        c += time.clock()
        t += time.time()

        self.print_tally()
        print ("Timing: %f process clock, %f wall clock" % (c, t))

    def _next_jobid(self):
        jobid = str(self._jobid)
        self._jobid += 1
        return jobid

    def _update_histogram(self, msg):
        k = msg.body
        self._histogram[k] = 1 + self._histogram.get(k, 0)

    def _record_timing_stats(self, msg):
        timings = msg.headers['timings']
        tdict = dict(timings)
        connect = tdict['connect']
        span = Span(connect, tdict['close'])
        self._slwin.insert(span)
        self._allspans.insert(span)
                
        for tag, t in timings:
            delta = t - connect
            self._timingstats.setdefault(tag, StatValue()).sample(delta)

    def _update_timing_stats(self):
        '''
        These stats use a simplistic second-wide bucket histogram.

        The concurrency statistic is sampled for every concurrency
        count in a given 1-second window throwing away the time
        information (even though it is recorded).

        Ex: Consider this sequence of connection spans:

        [(0.1, 0.9),
         (1.4, 1.5),
         (2.0, 3.1),
         (3.0, 3.1)]

        -then the concurrency windows would look a sequence of these
         samples (without times):
        [[0, 1],
         [0, 1],
         [1],
         [2, 0]]
        '''
        concvec = list(self._slwin.concurrency_vector())

        for window, subseq in self._slwin.as_bins(binwidth=1.0):
            self._rps.sample(len(subseq))
            for span in subseq:
                self._roundtrip.sample(span.magnitude)
            while concvec and window.contains(concvec[0][0]):
                _, _, q = concvec.pop(0)
                self._cps.sample(len(q))
        

class ProtocolError (Exception):
    def __init__(self, tmpl, *args):
        Exception.__init__(self, tmpl % args)


# Plugin interface:
queenbee_cls = HTTPQueenBee
workerbee_cls = HTTPWorkerBee


def add_options(parser):
    parser.add_option('--host', default='dummy', dest='http_host',
                        help=("Connect to the target HTTP host."
                              " The value 'dummy' (which is default)"
                              " does not connect to any server and behaves"
                              " as if an HTTP 200 OK response was received"
                              " for all requests."))

    parser.add_option('--port', default=80, dest='http_port', type='int',
                        help="Connect to the target HTTP port.")

    parser.add_option('--dump', default=None, dest='http_dump',
                        help="Results dump file.")


#!/usr/bin/python
#
# $LicenseInfo:firstyear=2010&license=mit$
# 
# Copyright (c) 2013, Linden Research, Inc.
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

import sys
import cPickle
import time

import apiary


class PreprocessedQueenBee(apiary.QueenBee):
    def __init__(self, options, arguments):
        apiary.QueenBee.__init__(self, options, arguments)
        
        self._tally = {}
        self._tally_time = time.time() + 15.0
        
        if len(arguments) > 1:
            print "'preprocessed' protocol can only handle 1 input file."
            sys.exit(1)
        
        self._file = open(arguments[0], 'rb')
        self._job_num = 0
        
    def tally(self, msg):
        # aggregate these error codes since we see a lot of them (1062/1064)
        if "Duplicate entry" in msg:
            msg = '501 (1062, "Duplicate entry for key")'
        if "You have an error in your SQL syntax" in msg:
            msg = '501 (1064, "You have an error in your SQL syntax")'
        self._tally[msg] = self._tally.get(msg, 0) + 1
        if time.time() > self._tally_time:
            self.print_tally()

    
    def print_tally(self):
        keys = self._tally.keys()
        keys.sort()
        print
        print "       count - message"
        print "------------   -------------------------------------------"
        for k in keys:
            print ("%12d - %s" % (self._tally[k], k))
        self._tally_time = time.time() + 15.0

        
    def next(self):
        try:
            message = cPickle.load(self._file)
            self._beekeeper.queenbee_start(0)
            self._send('worker-job', message)
            self._beekeeper.queenbee_end(0)
            self.tally("100 Job queued")
            return True

        except EOFError:
            return False
    
    def result(self, seq, d):
        self.tally(d)
    
        
    def main(self):
        apiary.QueenBee.main(self)
        self.print_tally()

        
    # Plugin interface:
queenbee_cls = PreprocessedQueenBee
workerbee_cls = None


def add_options(parser):
    pass

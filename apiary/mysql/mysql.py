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

import MySQLdb
import os
import re
import sys
import time

import apiary
from apiary.tools.debug import *
import sqllog
from apiary.tools import timestamp

def now(future=0.0):
    return timestamp.TimeStamp(time.time() + future)

def wait_until(ts):
    tn = now()
    if ts > tn:
        delta = float(ts - tn)
        # why was this ever here? --Lex
        #if delta > 65.0:
        #    raise Exception("trying to wait longer than 65s. now = %s, until = %s"
        #        % (str(tn), str(ts)))
        time.sleep(delta)

mysql_host = 'localhost'
mysql_port = 3306
mysql_user = 'guest'
mysql_passwd = ''
mysql_db = 'test'

class MySQLWorkerBee(apiary.WorkerBee):
    def __init__(self, options, arguments):
        apiary.WorkerBee.__init__(self, options, arguments)
        self._connect_options = {}
        self._connect_options['host'] = options.mysql_host
        self._connect_options['port'] = options.mysql_port
        self._connect_options['user'] = options.mysql_user
        self._connect_options['passwd'] = options.mysql_passwd
        self._connect_options['db'] = options.mysql_db
        self._connection = None
        self._no_mysql = options.no_mysql
    
    @traced_method
    def start(self):
        self._error = False
        self._errormsg = ''
        if self._no_mysql:
            return

        try:
            if self._connection is None and not self._no_mysql:
                connection = MySQLdb.connect(**self._connect_options)
                self._connection = connection
            pass
        except Exception, e: # more restrictive error catching?
            self._error = True
            self._errormsg = "500 " + str(e)
        
    @traced_method
    def event(self, data):
        if self._error:
            return
        try:
            (tstr, sql) = data.split("\t", 1)
            if not self._asap:
                self.log("waiting")
                wait_until(timestamp.TimeStamp(tstr))
            sql = sql.strip()
            if sql:
                self.log("executing SQL: " + sql)
                self.execute_sql(sql)  
        except Exception, e: # more restrictive error catching?
            self._error = True
            self._errormsg = "501 " + str(e)
        
    @traced_method
    def end(self):
        if self._no_mysql:
            return "200 OK"

        try:
            if True:
                self._connection.close()
                self._connection = None
        except Exception, e:
            if not self._error:
                self._error = True
                self._errormsg = "502 " + str(e)
        if self._error:
            return self._errormsg
        return '200 OK'

    @traced_method
    def execute_sql(self, sql):
        """Execute an SQL statement.
        
        Subclasses may override this to alter the SQL before being executed.
        If so, they should call this inherited version to actually execute
        the statement. It is acceptable for a sub-class to call this version
        multiple times.
        
        Exceptions are caught in the outer calling function (event())
        
        """
        if self._no_mysql:
            return
        cursor = self._connection.cursor()
        cursor.execute(sql)
        try:
            cursor.fetchall()
        except:
            pass # not all SQL has data to fetch
        cursor.close()
        

class MySQLQueenBee(apiary.QueenBee):
    def __init__(self, options, arguments):
        apiary.QueenBee.__init__(self, options, arguments)
        self._events = sqllog.input_events(arguments)
        # this builds a sequence of events from the log streams in the
        # arguments, which come here from the command line
        self._connections = {}
        self._tally = {}
        self._time_scale = 1.0 / options.speedup
        self._event_start = None
        self._replay_start = None
        self._tally_time = time.time() + 15.0
        
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

        
    @traced_method
    def next(self):
        try:
            while True:
                e = self._events.next() # e is a sqllog.Event object
                if e.state != sqllog.Event.Response:
                    break
        except StopIteration:
            return False
        
        if self._event_start is None:
            self._event_start = e.time
        t = (e.time - self._event_start) * self._time_scale + self._replay_start
        
        id = e.id
        if e.state == sqllog.Event.End:
            if id in self._connections:
                self.tally("102 End connection")
                del self._connections[id]
                self.end(id)
            else:
                self.tally("103 Duplicate end")
        else:
            if id not in self._connections:
                self.tally("100 Start connection")
                s = self._connections[id] = True
                self.start(id)
            self.tally("101 Event")
            self.event(id, str(t) + "\t" + e.body)

        return True
    
    def result(self, seq, d):
        self.tally(d)
    
        
    @traced_method
    def main(self):
        t = - time.time()
        c = - time.clock()
        self._replay_start = now(1.0)
        apiary.QueenBee.main(self)
        c += time.clock()
        t += time.time()

        print ("Timing: %f process clock, %f wall clock" % (c, t))
        self.print_tally()


# Plugin interface:
queenbee_cls = MySQLQueenBee
workerbee_cls = MySQLWorkerBee


def add_options(parser):
    parser.add_option('--no-mysql', default=False, dest='no_mysql', action='store_true',
                        help="Don't make mysql connections.  Return '200 OK' instead.")
    parser.add_option('--speedup', default=1.0, dest='speedup', type='float',
                        help="Time multiple used when replaying query logs.  2.0 means "
                             "that queries run twice as fast (and the entire run takes "
                             "half the time the capture ran for).")
    parser.add_option('--mysql-host',
                        default=mysql_host, metavar='HOST',
                        help='MySQL server to connect to (default: %default)')
    parser.add_option('--mysql-port',
                        default=mysql_port, metavar='PORT',
                        help='MySQL port to connect on (default: %default)')
    parser.add_option('--mysql-user',
                        default=mysql_user, metavar='USER',
                        help='MySQL user to connect as (default: %default)')
    parser.add_option('--mysql-passwd',
                        default=mysql_passwd, metavar='PW',
                        help='MySQL password to connect with (default: %default)')
    parser.add_option('--mysql-db',
                        default=mysql_db, metavar='DB',
                        help='MySQL database to connect to (default: %default)')


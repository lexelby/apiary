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
import re
import time

import hive
import sqllog
import timestamp

def now(future=0.0):
    return timestamp.TimeStamp(time.time() + future)

def wait_until(ts):
    tn = now()
    if ts > tn:
        delta = float(ts - tn)
        if delta > 65.0:
            raise Exception("trying to wait longer than 65s. now = %s, until = %s"
                % (str(tn), str(ts)))
        time.sleep(delta)

mysql_host = 'localhost'
mysql_port = 3306
mysql_user = 'guest'
mysql_passwd = ''
mysql_db = 'test'

class MySQLWorker(hive.Worker):
    def __init__(self, options, arguments):
        hive.Worker.__init__(self, options, arguments)
        self._asap = options.asap
        self._error = False
        self._errormsg = ''
        self._connect_options = {}
        self._connect_options['host'] = options.mysql_host
        self._connect_options['port'] = options.mysql_port
        self._connect_options['user'] = options.mysql_user
        self._connect_options['passwd'] = options.mysql_passwd
        self._connect_options['db'] = options.mysql_db
        self._connection = None
    
    def start(self):
#        self.log("starting")
        self._error = False
        self._errormsg = ''
        try:
            if self._connection is None:
#                self.log("making MySQL connection")
                connection = MySQLdb.connect(**self._connect_options)
                self._connection = connection
            pass
        except Exception, e: # more restrictive error catching?
            self._error = True
            self._errormsg = "500 " + str(e)
        
    def event(self, data):
#        self.log("event")
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
#                self.log("finished SQL")
        except Exception, e: # more restrictive error catching?
            self._error = True
            self._errormsg = "501 " + str(e)
        
    def end(self):
#        try:
#            self.log("committing")
#            self._connection.commit() # unclear if this should be here
#        except Exception, e:
#            pass
        try:
            if True:
#                self.log("closing")
                self._connection.close()
                self._connection = None
        except Exception, e:
            if not self._error:
                self._error = True
                self._errormsg = "502 " + str(e)
#        self.log("ended")
        if self._error:
            return self._errormsg
        return '200 OK'

    def execute_sql(self, sql):
        """Execute an SQL statement.
        
        Subclasses may override this to alter the SQL before being executed.
        If so, they should call this inherited version to actually execute
        the statement. It is acceptable for a sub-class to call this version
        multiple times.
        
        Exceptions are caught in the outer calling function (event())
        
        """
        cursor = self._connection.cursor()
        cursor.execute(sql)
        try:
            cursor.fetchall()
        except:
            pass # not all SQL has data to fetch
        cursor.close()
        

class MySQLCentral(hive.Central):
    def __init__(self, options, arguments):
        hive.Central.__init__(self, options, arguments)
        self._events = sqllog.input_events(arguments)
            # this builds a sequence of events from the log streams in the
            # arguments, which come here from the command line
        self._connections = {}
        self._tally = {}
        self._time_offset = None
        self._tally_time = time.time() + 15.0
        
    def tally(self, msg):
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
            while True:
                e = self._events.next() # e is a sqllog.Event object
                if e.state != sqllog.Event.Response:
                    break
        except StopIteration:
            return False
        
        if self._time_offset is None:
            self._time_offset = now(1.0) - e.time
        t = self._time_offset + e.time
        
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
    
        
    def main(self):
        t = - time.time()
        c = - time.clock()
        hive.Central.main(self)
        c += time.clock()
        t += time.time()

        # This never actually happens because hive.Central.main(self) never completes.  
        # Figure out how to force it to say it exited neatly?
        print ("Timing: %f process clock, %f wall clock" % (c, t))
        self.print_tally()


class MySQLHive(hive.Hive):
    def __init__(self, central_cls=MySQLCentral, worker_cls=MySQLWorker):
        hive.Hive.__init__(self, central_cls, worker_cls)
    
    def add_options(self, parser):
        hive.Hive.add_options(self, parser)
        parser.add_option('--asap',
                            action='store_true', default=False,
                            help='run SQL connections as fast as possible (default: off)')
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

if __name__ == '__main__':
    MySQLHive().main()    

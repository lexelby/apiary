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
This module implements a MySQL client as a WorkerBee.
'''

import re
import random
import socket
import sys
import cPickle
import MySQLdb
import apiary
import optparse
import warnings


class MySQLWorkerBee(apiary.WorkerBee):
    """A WorkerBee that sends transactions to MySQL"""

    COMMON_ERRORS = {
                        1062: "duplicate entry for key",
                        1064: "SQL syntax error",
                        1227: "access denied",
                        1146: "table does not exist"
                    }

    def __init__(self, options, *args, **kwargs):
        super(MySQLWorkerBee, self).__init__(options, *args, **kwargs)

        self._connect_options = {}
        self._connect_options['host'] = options.mysql_host
        self._connect_options['port'] = options.mysql_port
        self._connect_options['user'] = options.mysql_user
        self._connect_options['passwd'] = options.mysql_passwd
        self._connect_options['db'] = options.mysql_db
        self._connect_options['read_timeout'] = options.mysql_read_timeout
        self.connection = None
        self._table_dne_re = re.compile('''\(1146, "Table '.*' doesn't exist"\)''')

        if options.mysql_host.startswith('@'):
            self.dynamic_host = True
            self.dynamic_host_file = options.mysql_host[1:]
        else:
            self.dynamic_host = False

    def error(self, msg):
        if isinstance(msg, MySQLdb.Error):
            # we need to compress error messages so that they don't screw up the
            # table

            code = msg.args[0]
            msg = self.COMMON_ERRORS.get(code) or "MySQL error %d" % code
        else:
            msg = str(msg)

        super(MySQLWorkerBee, self).error(msg)

    def start_job(self, job_id):
        if not self.debug:
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)

        try:
            try:
                self.connection = MySQLdb.connect(**self._connect_options)
            except TypeError, e:
                # Using a python-mysqldb version that does not have
                # read_timeout patch from
                # http://sourceforge.net/p/mysql-python/patches/75/
                del self._connect_options['read_timeout']
                self.connection = MySQLdb.connect(**self._connect_options)
        except Exception, e:
            self.error(e)
            self.connection = None

    def send_request(self, query):
        if self.connection and query:
            try:
                cursor = self.connection.cursor()

                # remove invalid cruft sometimes erroneously added by pt-query-digest
                query = query.replace('\x00mysql_native_password', '')

                rows = cursor.execute(query.strip())
                if rows:
                    cursor.fetchall()
                cursor.close()
                return True
            except Exception, e:  # TODO: more restrictive error catching?
                self.error(e)

                try:
                    cursor.close()
                    self.connection.close()
                    self.connection = None
                except:
                    pass

                return False

    def finish_job(self, job_id):
        if self.connection:
            try:
                # Sometimes pt-query-digest neglects to mention the commit.
                cursor = self.connection.cursor()
                cursor.execute('COMMIT;')
                cursor.close()
            except:
                pass

            try:
                self.connection.close()
            except:
                pass

        self.connection = None


WorkerBee = MySQLWorkerBee


def add_options(parser):
    g = optparse.OptionGroup(parser, 'MySQL options (--protocol mysql)')
    g.add_option('--mysql-host',
                      default="localhost", metavar='HOST',
                      help='MySQL server to connect to (default: %default)')
    g.add_option('--mysql-port',
                      default=3306, type='int', metavar='PORT',
                      help='MySQL port to connect on (default: %default)')
    g.add_option('--mysql-user',
                      default='guest', metavar='USER',
                      help='MySQL user to connect as (default: %default)')
    g.add_option('--mysql-passwd',
                      default='', metavar='PW',
                      help='MySQL password to connect with (default: %default)')
    g.add_option('--mysql-db',
                      default='test', metavar='DB',
                      help='MySQL database to connect to (default: %default)')
    g.add_option('--mysql-read-timeout',
                      default=10, type='int', metavar='SECONDS',
                      help='MySQL client read timeout (default: 10)')
    parser.add_option_group(g)

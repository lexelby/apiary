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

import filtertools
import hive
import hive_mysql
import sqlfilters

class IndraNullWorker(hive_mysql.MySQLWorker):
    def __init__(self, options, arguments):
        hive_mysql.MySQLWorker.__init__(self, options, arguments)
        self._first_filters = []
        self._all_filters = []
        self._at_start = True
        
        if options.f_schema:
            self._first_filters.append(
                sqlfilters.PrependSchema(options.mysql_db))
    
    def start(self):
        #hive_mysql.MySQLWorker.start(self)
        self._at_start = True
    
    def event(self, data):
        hive_mysql.MySQLWorker.event(self, data)
        self._at_start = False

    def end(self):
        return "200 OK"

    def execute_sql(self, sql):
        statements = [sql]
        if self._at_start:
            statements = filtertools.filterthru(statements, self._first_filters)
        statements = filtertools.filterthru(statements, self._all_filters)
        for s in statements:
            #hive_mysql.MySQLWorker.execute_sql(self, s)
            pass

class IndraNullCentral(hive_mysql.MySQLCentral):
    def __init__(self, options, arguments):
        hive_mysql.MySQLCentral.__init__(self, options, arguments)

    def start(self, seq):
        pass

    def event(self, seq, data):
        pass

    def end(self, seq):
        pass

class IndraNullHive(hive_mysql.MySQLHive):
    def __init__(self):
        hive_mysql.MySQLHive.__init__(self, worker_cls=IndraNullWorker) #, central_cls=IndraNullCentral)
        #hive_mysql.MySQLHive.__init__(self, worker_cls=IndraNullWorker, central_cls=IndraNullCentral)
    
    def add_options(self, parser):
        hive_mysql.MySQLHive.add_options(self, parser)
        parser.add_option('--f-schema',
                            action='store_true', default=False,
                            help='infer schema for statements (default: off)')

if __name__ == '__main__':
    IndraNullHive().main()    

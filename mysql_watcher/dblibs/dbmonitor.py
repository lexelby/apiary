#!/usr/bin/env python
"""\
@file dbmonitor.py
@brief Script to track database health

$LicenseInfo:firstyear=2006&license=internal$

Copyright (c) 2006-2010, Linden Research, Inc.

The following source code is PROPRIETARY AND CONFIDENTIAL. Use of
this source code is governed by the Linden Lab Source Code Disclosure
Agreement ("Agreement") previously entered between you and Linden
Lab. By accessing, using, copying, modifying or distributing this
software, you acknowledge that you have been informed of your
obligations under the Agreement and agree to abide by those obligations.

ALL LINDEN LAB SOURCE CODE IS PROVIDED "AS IS." LINDEN LAB MAKES NO
WARRANTIES, EXPRESS, IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY,
COMPLETENESS OR PERFORMANCE.
$/LicenseInfo$
"""


#
# Utility classes that allow us to monitor and keep track of databases
#
import copy
import MySQLdb
from dbutil import *

MAX_SLAVE_BEHIND = 2 # Maximum number of seconds a slave can be behind
MAX_SLAVE_RUNNING = 4 # Maximum number of running processes allowed on the slave
MAX_SLAVE_AVG_AGE = 2 # Maximum average for all running queries


def safe_delete(host, delete_sql, num_rows = 0, verbose = False, stop_time = 0, paranoid = False, batch = True, ignored_slaves = ()):
    """Safely do a bulk delete using the delete string on the specified database host
    Returns the number of rows deleted, or -1 if there was an error"""

    # Validate the incoming statement
    if re.compile(".*\s+limit\s+\d+").match(delete_sql.lower()):
        print "Query '%s' appears to have a LIMIT clause, aborting" % delete_sql
        return -1

    dbm = DBMonitor(host)
    dbm.mSafeParanoidSlaves = paranoid
    dbm.mIgnoredSlaves = ignored_slaves
    deleted = 0

    MIN_ROWS = 500 # Minimum number of rows to delete at once
    MAX_ELAPSED = 15.0 # Elapsed time to target for a single delete

    # Dynamically adapt these variables to ensure optimum delete rates
    limit_rows = MIN_ROWS
    sleep_time = 1.0
    while 1:
        # Exit or continue based on the stop time
        if stop_time and (stop_time < time.time()):
            # Always print this
            if verbose:
                print "Time has run out, %d deletes completed!" % deleted
            return ("Timeout", deleted)

        safe = dbm.safeToRunQuery(verbose)
        if verbose:
            print time.ctime()
            dbm.dump()
        sleep_time = 1.0 # Sleep for 1 second between attempts, by default

        # Run the query if safe
        if safe:
            db = dbm.connect(True)
            if db:
                cursor = db.cursor()
                if batch:
                    full_query = delete_sql + " LIMIT %d" % limit_rows
                else:
                    full_query = delete_sql
                if verbose:
                    print "Executing %s" % full_query
                    sys.stdout.flush()
                begin_time = time.time()
                cursor.execute(full_query)
                end_time = time.time()
                elapsed = end_time - begin_time

                # Exit or continue based on number of rows deleted
                if cursor.rowcount < 0:
                    return ("Error", deleted)
                else:
                    # Increment the number of deleted rows
                    deleted += cursor.rowcount

                if cursor.rowcount < limit_rows:
                    # We've deleted everything, as otherwise we would have deleted limit_rows
                    return ("OK", deleted)

                if num_rows and (deleted >= num_rows):
                    # If we've specified a number of rows to try to delete and we've hit it, exit
                    return ("OK", deleted)

                # Adapt the number of rows to delete based on execution time
                limit_rows = limit_rows*(MAX_ELAPSED/elapsed)
                limit_rows = int((limit_rows//100)*100)
                limit_rows = max(MIN_ROWS, limit_rows)

                # Clamp us to delete exactly the specified number of rows if we have num_rows
                if num_rows:
                    limit_rows = min(num_rows - deleted, limit_rows)
                
                # Sleep for as long as the delete took before trying again, this should
                # give us a reasonable sleep time
                sleep_time = min(2*MAX_SLAVE_BEHIND, max(0.25, elapsed))
                if verbose:
                    print time.ctime()
                    print "Total deleted: %d" % deleted
                    print "Elapsed time: %s" % elapsed
                cursor.close()
        if verbose:
            print
            sys.stdout.flush()
        time.sleep(sleep_time)

def log_delete(host, query, end_time, table, paranoid = True, verbose = False, batch = True):
    """A convenience function for cleanups inside cron jobs"""
    print time.ctime(), "Deleting from %s" % table
    (status, count) = safe_delete(host, query, verbose = verbose, stop_time = end_time, paranoid = paranoid, batch = batch)
    if "Error" == status:
        print >> sys.stderr, time.ctime(), "%s: %s failed" % (table, query)
        return True
    elif "Timeout" == status:
        print >> sys.stderr, time.ctime(), "%s: Timeout, %d rows deleted" % (table, count)
        return True
    else:
        print time.ctime(), "%s: %d rows deleted" % (table, count)
        return False

class DBMonitor:
    """Monitor a DB and all of its slaves for health"""
    def __init__(self, host, user, password):
        # Generic database information
        self.mHost = host
        self.mUser = user
        self.mPassword = password
        self.mAlive = False
        self.mNumProcesses = 0
        self.mNumRunning = 0
        self.mNumSleeping = 0
        self.mAvgAge = 0.0
        self.mIsSlave = False
        self.mDB = None
        self.mProcessRows = None # Contains the results of the last process row queries

        # Master specific info
        self.mSlaves = None # A list of all slaves

        # Slave specific info
        self.mMaster = ""
        self.mSlaveRunning = False
        self.mSlaveBehind = 0

        self.mIgnoredSlaves = () # A list of slaves to ignore

        # Database "health" options
        self.mSafeParanoidSlaves = True # All slaves have to be healthy
        self.mSafeMaxProcesses = 5      # Max processes to allow to run on this host when safe
        self.mSafeMaxAvgAge = 300       # If multiple queries, maximum allowed average age
        
    def connect(self, force_reconnect = False):
        """Connect to the database - flush the existing connection if we're already connected."""
        if self.mDB and force_reconnect:
            self.mDB.close()
            self.mDB = None

        if not self.mDB:
            try:
                self.mDB = MySQLdb.connect(self.mHost,
                                           self.mUser,
                                           self.mPassword)
            except:
                print "Failed to connect to database on %s" % self.mHost
                self.mDB = None
                self.mAlive = False
                return None

            self.mDB.autocommit(True)

        self.mAlive = True
        return self.mDB
    def disconnect(self):
        self.mDB.close()
        self.mDB = None
    def killProcess(self, pid):
        self.connect(force_reconnect=False)
        if self.mDB:
            cursor = self.mDB.cursor()
            try:
                cursor.execute("kill %d" % pid)
            except:
                pass
            cursor.close()
        else:
            print "Couldn't get database"

    def getProcessList(self):
        """Return all of the running processes on the DB in a sequence of row maps."""

        # Flush existing statistics
        self.mNumProcesses = 0
        self.mNumRunning = 0
        self.mNumSleeping = 0
        
        # Connect to the DB
        self.connect()
        if not self.isAlive():
            return ()
        cursor = self.mDB.cursor()
        cursor.execute("show full processlist")
        all = all_as_maps(cursor)
        cursor.close()

        # Generate statistics
        self.mNumProcesses = len(all)
        running_ages = 0.0

        self.mProcessRows = all

        # Accumulate statistics for all processes
        for row in all:
            # Ignore backups
            if (row['Command'] == 'Query') and (row['User'] != 'backup'):
                self.mNumRunning += 1
                running_ages += row['Time']
            elif row['Command'] == 'Sleep':
                self.mNumSleeping += 1
        self.mAvgAge = running_ages/self.mNumRunning
        return all

    def explain(self, database, query):
        self.connect()
        cursor = self.mDB.cursor()
        if database:
            cursor.execute("use %s" % database)
        query = query.replace('\n',' ')
        query = re.sub('\s+', ' ', query)
        explain_str = "explain %s" % query

        out = {}
        try:
            cursor.execute(explain_str)
        except:
            #print "Exception in explain for db: %s query:%s" % (database, query)
            return None
        all = all_as_maps(cursor)
        # Reorganize join info by query
        out['explain_str'] = explain_str
        out['tables'] = {}
        explain_tables = out['tables']
        out['analysis'] = {}
        out['raw_explain'] = copy.deepcopy(all)
        for row in all:
            table = row['table']
            if not table:
                table = ''
            #if table in explain_tables:
            #    continue
            explain_tables[table] = copy.deepcopy(row)
            for key in row.keys():
                if None == key:
                    raise "Bad row:", row
            if not row['possible_keys']:
                if not 'no_key' in out['analysis']:
                    out['analysis']['no_key'] = []
                out['analysis']['no_key'].append(table)
        return out

    def safeToRunQuery(self, verbose = False):
        """Return if it's safe to run an expensive query"""
        # Get updates on the master
        self.getProcessList()
        
        # Get updates on all of the slaves
        if not self.mSlaves:
            self.getSlaves()
        self.updateSlaves()

        # Now, check everything we care about
        MAX_PROCESSES = 5 # Maximum number of running processes on the master

        safe = True
        if self.mNumRunning > self.mSafeMaxProcesses:
            safe = False
            if verbose:
                print "%s: Unsafe: %d running processes" % (self.mHost, self.mNumRunning)
        if self.mAvgAge > self.mSafeMaxAvgAge:
            safe = False
            if verbose:
                print "%s: Unsafe: %f average query age" % (self.mHost, self.mAvgAge)

        num_slaves = self.mSlaves
        # Check if slaves are OK, but only if we have some
        if num_slaves:
            healthy_slaves = 0
            for slave in self.mSlaves.values():
                if slave.isSlaveHealthy(verbose):
                    healthy_slaves += 1
                elif slave.mHost in self.mIgnoredSlaves:
                    # Pretend the slave is healthy
                    healthy_slaves += 1
                    if verbose:
                        print "Ignoring unhealthy slave %s" % slave.mHost


            if ((self.mSafeParanoidSlaves and (healthy_slaves != len(self.mSlaves)))
                or (not healthy_slaves)):
                safe = False
                if verbose:
                    print "Not enough healthy slaves (%d/%d)" % (healthy_slaves, len(self.mSlaves))
        
        if not safe and verbose:
            print "%s: Unsafe to run query!" % self.mHost
        elif verbose:
            print "%s: Safe to run query" % self.mHost
        return safe

    def masterStatusString(self):
        return "%s: Master: (%d/%d) AvgAge %.2f" % (self.mHost, self.mNumRunning, self.mNumProcesses, self.mAvgAge)

    def dump(self):
        """Dump useful and interesting information about this database and its slaves"""
        print self.masterStatusString()
        for slave in self.mSlaves.values():
            slave.dumpSlave()

    def isAlive(self):
        return self.mAlive

    #
    # Get all table information for a database
    #
    def getTableInfo(self):
        # Connect to the DB
        self.connect()
        if not self.isAlive():
            return ()
        cursor = self.mDB.cursor()
        cursor.execute("show databases")
        all_dbs = all_as_maps(cursor)
        dbs = {}
        # Get all the databases
        for db in all_dbs:
            dbs[db['Database']] = {}

        bad_tables = ['tmp', 'information_schema', 'mysql','secondopinion']
        # Iterate through all the databases and get table information.
        for db in dbs.keys():
            dbs[db]['tables'] = {}
            if db in bad_tables:
                continue
            print "Database:", db
            print "----------------------"
            cursor.execute("use %s" % db)
            cursor.execute("show table status")
            all_tables = all_as_maps(cursor)
            print "Name Rows Data Index"
            print "---------------------"
            for table in all_tables:
                dbs[db]['tables'][table['Name']] = copy.deepcopy(table)
                print table['Name'],table['Rows'], table['Data_length'], table['Index_length']
            print
        return dbs

    #
    # Get all tables and their fields for the sequence and indra DBs
    # as a nested dict
    def getTableFields(self):
        # Connect to the DB
        self.connect()
        if not self.isAlive():
            return ()
        cursor = self.mDB.cursor()
        dbs = {'sequence':{},'indra':{}}
        # Iterate through all the databases and get table information.
        for db in dbs.keys():
            cursor.execute("use %s" % db)
            cursor.execute("show table status")
            all_tables = all_as_maps(cursor)
            for table in all_tables:
                cursor.execute("desc %s" % table['Name'])
                dbs[db][table['Name']] = [row[0] for row in cursor.fetchall()]
        return dbs

    #
    # Slave management methods for master
    #
    def getSlaves(self):
        """Get a list of all of the slaves of this mysql host"""
        procs = self.getProcessList()
        self.clearSlaves()
        for row in procs:
            if row['Command'] == 'Binlog Dump':
                host, port = row['Host'].split(':')
                hostname = socket.gethostbyaddr(host)[0]
                slave = DBMonitor(hostname, self.mUser, self.mPassword)
                slave.mIsSlave = True
                self.mSlaves[hostname] = slave
        #print self.mSlaves

    def clearSlaves(self):
        """Cleanup all slave DBs"""
        self.mSlaves = {}

    def updateSlaves(self):
        """Update the status of all of the slave DBs"""
        for slave in self.mSlaves.values():
            slave.updateSlaveStatus()
        pass

    #
    # Slave management methods for slave
    #
    def updateSlaveStatus(self):
        """Get information about slave status"""

        # Flush existing data
        self.mMaster = ""
        self.mSlaveRunning = False
        self.mSlaveBehind = 0
        
        # Connect to the database
        self.connect()
        if not self.isAlive():
            return
        cursor = self.mDB.cursor()
        cursor.execute("show slave status")
        all = all_as_maps(cursor)
        cursor.close()

        # Pull data from result
        row = all[0]
        self.mMaster = row['Master_Host']
        self.mSlaveRunning = ('Yes' == row['Slave_SQL_Running']) and ('Yes' == row['Slave_IO_Running'])
        self.mSlaveBehind = row['Seconds_Behind_Master']
        if None == self.mSlaveBehind:
            self.mSlaveBehind = 99999

        # Update process list stats
        self.getProcessList()

    def isSlaveHealthy(self, verbose = False):
        healthy = True
        if not self.isAlive():
            healthy = False
            if verbose:
                print "%s: Unable to connect to database!" % self.mHost
        if not self.mSlaveRunning:
            healthy = False
            if verbose:
                print "%s: Slave is not running!" % self.mHost
        if self.mSlaveBehind > MAX_SLAVE_BEHIND:
            healthy = False
            if verbose:
                print "%s: Slave is %d seconds behind" % (self.mHost, self.mSlaveBehind)
        if self.mNumRunning > MAX_SLAVE_RUNNING:
            healthy = False
            if verbose:
                print "%s: Slave has %d running processes" % (self.mHost, self.mNumRunning)
        #if self.mAvgAge > MAX_SLAVE_AVG_AGE:
        #   healthy = False
        #   if verbose:
        #       print "%s: Slave has %f average age" % (self.mHost, self.mAvgAge)
        return healthy

    def slaveStatusString(self):
        return "%s: Slave: Run: %s\tBehind: %d\tRun Procs: %d\tAvgAge: %.2f" \
               % (self.mHost, str(self.mSlaveRunning), self.mSlaveBehind, self.mNumRunning, self.mAvgAge)
        
    def dumpSlave(self):
        """Dump slave-specific info"""
        print self.slaveStatusString()

def main():
    """Simple test stub which dumps how happy a particular database is."""
    db_host = sys.argv[1]
    dbm = DBMonitor(db_host)
    while 1:
        dbm.safeToRunQuery(True)
        dbm.dump()
        print
        time.sleep(1.0)

if __name__ == "__main__":
    main()

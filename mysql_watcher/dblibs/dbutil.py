#!/usr/bin/env python
"""\
@file dbutil.py
@brief Various utility methods to process database-related stuff.

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
import array
import binascii
import gzip
import math
import os
import re
import socket
import string
import struct
import sys
import time

from indra.base import llsd

def asciify(str):
    "Lame ASCIIfication of a string to keep various things from barfing"
    out_str = ""
    for ch in str:
        if (ch >= chr(0x9)) and (ch <= '~'):
            out_str += ch
        else:
            out_str += "."
    return out_str

def all_as_maps(cursor):
    """Return all of the cursor with maps for each row instead of sequences"""
    all_seq = cursor.fetchall()
    ret_all = []
    descs = cursor.description
    for row in all_seq:
        new_row = {}
        count = 0
        for desc in descs:
            new_row[desc[0]] = row[count]
            count += 1
        ret_all.append(new_row)
    return ret_all

#
# Cache IP to string lookup to make it faster
#
ip_table = {}
def lookup_ip_string(ip_bin):
    if not ip_bin in ip_table:
        ip_table[ip_bin] = "%d.%d.%d.%d" % ((ip_bin & 0xff000000L) >> 24,
                                            (ip_bin & 0x00ff0000L) >> 16,
                                            (ip_bin & 0x0000ff00L) >> 8,
                                            ip_bin & 0x000000ffL)
    return ip_table[ip_bin]

def llquery_from_llsd(query_llsd):
    # Hack, fill in arbitary data for info that isn't serialized
    query = LLQuery(None, None, query_llsd['query'], 0.0)
    query.mData['host_clean'] = query_llsd['host_clean']
    query.mData['query_clean'] = query_llsd['query_clean']

    # Hack, keeps correctOutliers from trashing the data
    #query.mNumQueries = query_llsd['num_queries']
    #query.mTotalTime = query_llsd['total_time']
    try:
        query.mNumQueriesCorrected = query_llsd['num_queries_corrected']
        query.mTotalTimeCorrected = query_llsd['total_time_corrected']
    except:
        # Hack for old output which didn't generate this data
        query.mNumQueriesCorrected = query_llsd['num_queries']
        query.mTotalTimeCorrected = query_llsd['total_time']
        
    return query

def get_query_tables(query):
    "Return the list of tables in a query"
    #
    # Really dumb method, literally iterates through a bunch of regular expressions to pull this out.
    # There are probably better methods out there.
    #
    
    out_tables = []
    # Clean up the query
    query = query.replace('\n',' ')
    query = re.sub('\s+', ' ', query)
    
    m = LLQuery.sSelectWhereRE.match(query)
    if m:
        # Split apart by commas
        tables = m.group(1).split(',')
        for table in tables:
            # Take the first part (which is table name)
            out_tables.append(string.strip(table.split()[0]))
        return out_tables
    
    m = LLQuery.sSelectRE.match(query)
    if m:
        out_tables.append(string.strip(m.group(1)))
        return out_tables
        
    m = LLQuery.sUpdateRE.match(query)
    if m:
        # Split apart by commas
        tables = m.group(1).split(',')
        for table in tables:
            # Take the first part (which is table name)
            out_tables.append(string.strip(table.split()[0]))
        return out_tables

    m = LLQuery.sReplaceRE.match(query)
    if m:
        out_tables.append(string.strip(m.group(1)))
        return out_tables
    
    m = LLQuery.sInsertRE.match(query)
    if m:
        out_tables.append(string.strip(m.group(1)))
        return out_tables

    m = LLQuery.sDeleteRE.match(query)
    if m:
        out_tables.append(string.strip(m.group(1)))
        return out_tables
    return out_tables


MIN_BIN=-15
MAX_BIN=10
class LLQuery:
    "Represents all of the data associated with a query"
    fromLLSDStats = staticmethod(llquery_from_llsd)
    def __init__(self, host, port, query, start_time):
        # Store information which will be serialized for metadata in a map
        self.mData = {}
        self.mData['host'] = host
        self.mData['port'] = port
        self.mData['query'] = query

        # Metadata
        self.mData['host_clean'] = None
        self.mData['host_full'] = None
        self.mData['query_clean'] = None
        self.mData['tables'] = []

        #
        # Stats information
        #
        self.mNumQueries = 0
        self.mTotalTime = 0.0
        self.mOutQueries = 0
        self.mTotalTimeCorrected = 0.0 # Corrected to remove outliers
        self.mNumQueriesCorrected = 0 # Corrected to remove outliers

        # LLQueryStatBins for the query time histogram, as well as corrected time
        # Query times are collected into bins based on power of 2 execution times (in seconds).
        # Each bin collects the number of queries and total execution time. See LLQueryStatBin
        # for more details
        self.mBins = {} # Bins for histogram

        # This stuff doesn't usually get serialized
        self.mQueryLen = len(query)
        self.mStartTime = start_time
        self.mResponseTime = start_time

    def __hash__(self):
        return (self.mData['host_clean'] + ":" + self.mData['query_clean']).__hash__()

    def __eq__(self, other):
        # Note, this matches on clean, not strictly correct
        if ((self.mData['query_clean'] == other.mData['query_clean']) and
            (self.mData['host_clean'] == other.mData['host_clean'])):
            return True
        return False

    def getKey(self):
        # The string key is just the clean host and query, concatenated
        return self.mData['host_clean'] + ":" + self.mData['query_clean']
        
    def clean(self):
        "Generate the clean query so it can be used for statistics"
        if not self.mData['host_clean']:
            (self.mData['host_clean'], self.mData['host_full']) = get_host_type(self.mData['host'])
            self.mData['query_clean'] = clean_query(self.mData['query'], 0)

    def getAvgTimeCorrected(self):
        "Average time per query, corrected for outliers"
        return self.mTotalTimeCorrected/self.mNumQueriesCorrected

    def queryStart(self):
        "When collecting query stats, use this when the query is receieved"
        self.mNumQueries += 1
        self.mOutQueries += 1

    def queryResponse(self, elapsed):
        "When collecting stats, use this when the response is received"
        self.mTotalTime += elapsed
        self.mOutQueries -=1

        # Determine which stat bin this query is in
        bin = MIN_BIN
        if elapsed:
            bin = int(math.log(elapsed,2))
        bin = max(MIN_BIN, bin)
        bin = min(MAX_BIN, bin)
        if bin not in self.mBins:
            self.mBins[bin] = LLQueryStatBin(bin)
        self.mBins[bin].accumulate(elapsed)

    def correctOutliers(self):
        "Find outliers bins and calculate corrected results"
        # Outlier bins have query counts which are 3 orders of magnitude less than the total count for that query
        if not self.mNumQueries:
            # FIXME: This is a hack because we don't save this information in the query count dump
            return
        min_queries = self.mNumQueries/100
        self.mTotalTimeCorrected = 0.0
        self.mNumQueriesCorrected = 0
        for i in self.mBins.keys():
            if self.mBins[i].mNumQueries < min_queries:
                # Outlier, flag as such.
                self.mBins[i].mOutlier = True
            else:
                self.mTotalTimeCorrected += self.mBins[i].mTotalTime
                self.mNumQueriesCorrected += self.mBins[i].mNumQueries
        if self.mNumQueriesCorrected == 0:
            #HACK: Deal with divide by zero
            self.mNumQueriesCorrected = 1

    # Miscellaneous regular expressions to analyze the query type
    sReadRE = re.compile("(SELECT.*)|(USE.*)", re.IGNORECASE)
    sSelectWhereRE = re.compile("\(?\s*?SELECT.+?FROM\s+\(?(.*?)\)?\s+WHERE.*", re.IGNORECASE)
    sSelectRE = re.compile("\(?\s*?SELECT.+?FROM\s+(.+)(?:\s+LIMIT.*|.*)", re.IGNORECASE)
    sUpdateRE = re.compile("UPDATE\s+(.+?)\s+SET.*", re.IGNORECASE)
    sReplaceRE = re.compile("REPLACE INTO\s+(.+?)(?:\s*\(|\s+SET).*", re.IGNORECASE)
    sInsertRE = re.compile("INSERT.+?INTO\s+(.+?)(?:\s*\(|\s+SET).*", re.IGNORECASE)
    sDeleteRE = re.compile("DELETE.+?FROM\s+(.+?)\s+WHERE.*", re.IGNORECASE)
    def analyze(self):
        "Does some query analysis on the query"
        query = self.mData['query_clean']
        self.mData['tables'] = get_query_tables(query)
        if 'type' in self.mData:
            # Already analyzed
            return
        if LLQuery.sReadRE.match(query):
            self.mData['type'] = 'read'
        else:
            self.mData['type'] = 'write'


    def dumpLine(self, elapsed, query_len = 0):
        "Dump a semi-human-readable stats line for reporting"
        bin_str = ''
        for i in range(MIN_BIN,MAX_BIN+1):
            if i in self.mBins:
                if self.mBins[i].mOutlier:
                    bin_str += '*'
                else:
                    bin_str += str(int(math.log10(self.mBins[i].mNumQueries)))
            else:
                bin_str += '.'
        if not query_len:
            query_len = 4096
        num_queries = self.mNumQueriesCorrected
        if not num_queries:
            num_queries = 1
        return ("%s\t%5d\t%6.2f\t%6.2f\t%1.4f\t%s\t" % (bin_str, num_queries,
                                                       num_queries/elapsed, self.mTotalTimeCorrected,
                                                       self.mTotalTimeCorrected/num_queries, self.mData['host_clean'])) \
                                                       + self.mData['query_clean'][0:query_len]

    def as_map(self):
        "Make an LLSD map version of data that can be used for merging"
        self.analyze()
        self.mData['num_queries'] = self.mNumQueries
        self.mData['total_time'] = self.mTotalTime
        self.mData['num_queries_corrected'] = self.mNumQueriesCorrected
        self.mData['total_time_corrected'] = self.mTotalTimeCorrected
        return self.mData

class LLConnStatus:
    "Keeps track of the status of a connection talking to mysql"
    def __init__(self, ip_port, start_time):
        self.mLastMysqlPacketNumber = 0
        self.mNumPackets = 0
        self.mIPPort = ip_port
        self.mStartTime = start_time
        self.mLastUpdate = start_time
        self.mCurState = ""
        self.mLastQuery = None
        self.mNumQueries = 0

    def quit(self, src_ip, src_port, pkt_time):
        query = LLQuery(src_ip, src_port, "Quit", pkt_time)
        query.clean()
        self.mLastUpdate = pkt_time
        self.mLastQuery = query
        self.mNumPackets += 1

    def queryStart(self, src_ip, src_port, pkt_time, raw, pkt_len, offset):
        query_len = pkt_len - 1
        query = LLQuery(src_ip, src_port, raw[offset:offset + (pkt_len - 1)], pkt_time)
        self.mLastUpdate = pkt_time
        # Packet length includes the command, offset into raw doesn't
        if query_len > (len(raw) - offset):
            query.mQueryLen = query_len
            self.mCurState = "SendingQuery"
        else:
            self.mCurState = "QuerySent"
            query.clean()
        self.mNumQueries += 1
        self.mLastQuery = query
        self.mNumPackets += 1

    def queryStartProcessed(self, src_ip, src_port, pkt_time, query_str):
        query = LLQuery(src_ip, src_port, query_str, pkt_time)
        query.clean()
        self.mLastUpdate = pkt_time
        self.mCurState = "QuerySent"
        self.mNumQueries += 1
        self.mLastQuery = query
        self.mNumPackets += 1

    def updateNonCommand(self, pkt_time, raw):
        # Clean up an existing query if you get a non-command.
        self.mNumPackets += 1
        self.mLastUpdate = pkt_time
        if self.mLastQuery:
            if self.mCurState == "SendingQuery":
                # We're continuing a query
                # We won't generate a new clean version, because it'll $!@# up all the sorting.
                self.mLastQuery.mData['query'] += raw
                if len(self.mLastQuery.mData['query']) == self.mLastQuery.mQueryLen:
                    self.mCurState = "QuerySent"
                    self.mLastQuery.clean()
                return
            else:
                #
                # A non-command that's continuing a query. Not sure why this is happening,
                # but clear the last query to avoid generating inadvertent long query results.
                #
                self.mLastQuery = None
        # Default to setting state to "NonCommand"
        self.mCurState = "NonCommand"

    def updateResponse(self, pkt_time, result_type):
        # If we've got a query running, accumulate the elapsed time
        start_query_response = False
        if self.mCurState == "QuerySent":
            lq = self.mLastQuery
            if lq:
                if lq.mStartTime == 0.0:
                    lq.mStartTime = pkt_time
                lq.mResponseTime = pkt_time
                start_query_response = True

        self.mLastUpdate = pkt_time
        if result_type == 0:
            self.mCurState = "Result:RecvOK"
        elif result_type == 0xff:
            self.mCurState = "Result:Error"
        elif result_type == 0xfe:
            self.mCurState = "Result:EOF"
        elif result_type == 0x01:
            self.mCurState = "Result:Header"
        else:
            self.mCurState = "Result:Data"
        return start_query_response

    def dump(self):
        if self.mLastQuery:
            print "%s: NumQ: %d State:%s\n\tLast: %s" % (self.mIPPort, self.mNumQueries, self.mCurState,
                                                         self.mLastQuery.mData['query_clean'][0:40])
        else:
            print "%s: NumQ: %d State:%s\n\tLast: None" % (self.mIPPort, self.mNumQueries, self.mCurState)

class LLQueryStatBin:
    "Keeps track of statistics for one query bin"
    def __init__(self, power):
        self.mMinTime = pow(2, power)
        self.mMaxTime = pow(2, power+1)
        self.mTotalTime = 0
        self.mNumQueries = 0
        self.mOutlier = False
    def accumulate(self, elapsed):
        self.mTotalTime += elapsed
        self.mNumQueries += 1

def dump_query_stat_header():
    return "LogHistogram (-15:10)     \tCount\tQPS\tTotal\tAvg\tHost\tQuery"


class LLQueryStatMap:
    def __init__(self, description, start_time):
        self.mDescription = description
        self.mQueryMap = {}
        self.mStartTime = start_time
        self.mFinalTime = 0
        self.mLastTime = self.mStartTime
        self.mQueryStartCount = 0
        self.mQueryResponseCount = 0

    def load(self, fn):
        "Load dumped query stats from an LLSD file"
        # Read in metadata
        in_file = open(fn)
        in_string = in_file.read()
        in_file.close()
        in_llsd = llsd.LLSD.parse(in_string)
        info = in_llsd[0]
        query_list = in_llsd[1]
        self.mDescription = info['description']
        self.mStartTime = info['start_time']
        self.mLastTime = info['last_time']
        self.mFinalTime = info['last_time']
        self.mQueryStartCount = info['query_start_count']
        self.mQueryResponseCount = info['query_response_count']
        # Iterate through all the queries, and populate the query map.
        for query_row in query_list:
            query = LLQuery.fromLLSDStats(query_row)
            self.mQueryMap[query.getKey()] = query

    def analyze(self):
        for query in self.mQueryMap.values():
            query.analyze()

    def queryStart(self, query):
        if not query in self.mQueryMap:
            #query.analyze()
            self.mQueryMap[query] = query
        self.mQueryMap[query].queryStart()
        # Update elapsed time for this map
        self.mLastTime = query.mStartTime
        if self.mLastTime < self.mStartTime:
            self.mStartTime = self.mLastTime
        if self.mLastTime > self.mFinalTime:
            self.mFinalTime = self.mLastTime
        self.mQueryStartCount += 1
        
    def queryResponse(self, query):
        if not query in self.mQueryMap:
            self.queryStart(query)
        elapsed = query.mResponseTime - query.mStartTime
        self.mQueryMap[query].queryResponse(elapsed)
        self.mLastTime = query.mResponseTime
        if self.mLastTime > self.mFinalTime:
            self.mFinalTime = self.mLastTime
        self.mQueryResponseCount += 1

    def getElapsedTime(self):
        return self.mFinalTime - self.mStartTime

    def getQPS(self):
        return self.mQueryStartCount / self.getElapsedTime()

    def correctOutliers(self):
        for query in self.mQueryMap.values():
            query.correctOutliers()

    def getSortedKeys(self, sort_by = "total_time"):
        "Gets a list of keys sorted by sort type"
        self.correctOutliers()
        
        items = self.mQueryMap.items()
        backitems = None

        if sort_by == "total_time":
            backitems = [[v[1].mTotalTimeCorrected, v[0]] for v in items]
        elif sort_by == "count":
            backitems = [[v[1].mNumQueriesCorrected, v[0]] for v in items]
        elif sort_by == "avg_time":
            backitems = [[v[1].getAvgTimeCorrected(), v[0]] for v in items]
        else:
            # Fallback, sort by total time
            backitems = [[v[1].mTotalTimeCorrected, v[0]] for v in items]

        backitems.sort()
        backitems.reverse()

        # Get the keys out of the items
        sorted = []
        for pair in backitems:
            sorted.append(pair[1])
        return sorted

    def getSortedStats(self, sort_by = "total_time", num_stats = 0):
        "Gets a list of the top queries according to sort type"
        sorted_keys = self.getSortedKeys(sort_by)

        if num_stats == 0:
            l = len(sorted_keys)
        else:
            l = min(num_stats, len(sorted_keys))

        stats = []
        for i in range(0, l):
            stats.append(self.mQueryMap[sorted_keys[i]])
        return stats

    def dumpStatus(self, sort_type = "total_time", elapsed = None):
        # Dump status according to total time
        if not elapsed:
            elapsed = self.getElapsedTime()

        sorted_stats = self.getSortedStats(sort_type)
        for query in sorted_stats:
            print query.dumpLine(elapsed, 60)

    def dumpLLSD(self, filename):
        # Analyze queries to generate metadata
        self.analyze()
        # Dump an LLSD document representing the entire object
        out = []

        # First, dump all the metadata into the first block
        info_map = {}
        info_map['description'] = self.mDescription
        info_map['start_time'] = self.mStartTime
        info_map['last_time'] = self.mLastTime
        info_map['query_start_count'] = self.mQueryStartCount
        info_map['query_response_count'] = self.mQueryResponseCount
        out.append(info_map)

        # Dump all of the query info into the second block
        sorted_stats = self.getSortedStats("total_time")
        query_list = []
        for query in sorted_stats:
            query_list.append(query.as_map())
        out.append(query_list)
        f = open(filename, "w")
        f.write(str(llsd.LLSD(out)))
        f.close()

    def dumpTiming(self, filename):
        cur_time = time.time()
        f = open(filename, "w")
        f.write(dump_query_stat_header() + "\n")
        # Sort the queries
        sorted_stats = self.getSortedStats("total_time")
        for query in sorted_stats:
            f.write(query.dumpLine(cur_time - self.mStartTime))
            f.write("\n")
        f.close()

    def dumpCountsLLSD(self, filename):
        "Dump the query statistics as an LLSD doc, for later merging with the query_info doc"

        out = []
        # Put the metadata into a map
        info_map = {}
        info_map['description'] = self.mDescription
        info_map['start_time'] = self.mStartTime
        info_map['last_time'] = self.mLastTime
        info_map['query_start_count'] = self.mQueryStartCount
        info_map['query_response_count'] = self.mQueryResponseCount
        out.append(info_map)

        sorted_stats = self.getSortedStats("total_time")
        query_list = []
        for query in sorted_stats:
            query_row = {}
            # We only want to dump identifying info and stats, not metadata
            query_row['host_clean'] = query.mData['host_clean']
            # Convert the queries to utf-8 to make sure it doesn't break XML
            try:
                u = unicode(query.mData['query_clean'])
                query_row['query_clean'] = u.encode('utf-8')
            except:
                query_row['query_clean'] = 'NON-UTF8'
            try:
                u = unicode(query.mData['query'])
                query_row['query'] = u.encode('utf-8')
            except:
                query_row['query'] = 'NON-UTF8'
            query_row['count'] = query.mNumQueriesCorrected
            query_row['total_time'] = query.mTotalTimeCorrected
            query_row['avg_time'] = query.getAvgTimeCorrected()
            query_list.append(query_row)

        out.append(query_list)
        f = open(filename, "w")
        f.write(str(llsd.LLSD(out)))
        f.close()


class LLBinnedQueryStats:
    "Keeps track of a fixed number of N minute bins of query stats"
    def __init__(self):
        self.mHourBins = {} # This will be keyed by unixtime seconds, eventually
        self.mMinuteBins = {}
        self.mLastUpdateHour = 0
        self.mLastUpdateMinute = 0

    def dumpTiming(self, path):
        # Dump hour bins
        for (key, value) in self.mHourBins.items():
            value.dumpTiming("%s/hour-%s-query_timing.txt" % (path, key))
        # Dump minute bins
        for (key, value) in self.mMinuteBins.items():
            value.dumpTiming("%s/minute-%s-query_timing.txt" % (path, key))

    def dumpCountsLLSD(self, path):
        # Dump hour bins
        for (key, value) in self.mHourBins.items():
            value.dumpCountsLLSD("%s/hour-%s-query_counts.llsd" % (path, key))
        # Dump minute bins
        for (key, value) in self.mMinuteBins.items():
            value.dumpCountsLLSD("%s/minute-%s-query_counts.llsd" % (path, key))

    def dumpLLSD(self, path):
        # Dump hour bins
        for (key, value) in self.mHourBins.items():
            value.dumpLLSD("%s/hour-%s-query_dump.llsd" % (path, key))
        # Dump minute bins
        for (key, value) in self.mMinuteBins.items():
            value.dumpLLSD("%s/minute-%s-query_dump.llsd" % (path, key))

    def flushOldBins(self, time_secs):
        for minute_bin_str in self.mMinuteBins.keys():
            bin_secs = time.mktime(time.strptime(minute_bin_str, "%Y-%m-%d-%H-%M"))
            if (time_secs - bin_secs) > 3*3600:
                del self.mMinuteBins[minute_bin_str]

    def queryStart(self, query):
        "Update associated bin for the time specified, creating if necessary"
        # Hour and minute bins
        t = time.localtime(query.mStartTime)
        hour_bin_str = time.strftime("%Y-%m-%d-%H", t)
        minute_bin_str = time.strftime("%Y-%m-%d-%H-%M", t)
        hour = t[3]
        minute = t[4]
        # FIXME: These start times are a bit inaccurate, but should be fine under heavy query load.
        if not hour_bin_str in self.mHourBins:
            self.mHourBins[hour_bin_str] = LLQueryStatMap(hour_bin_str, query.mStartTime)
        if not minute_bin_str in self.mMinuteBins:
            self.mMinuteBins[minute_bin_str] = LLQueryStatMap(minute_bin_str, query.mStartTime)

        self.mHourBins[hour_bin_str].queryStart(query)
        self.mMinuteBins[minute_bin_str].queryStart(query)

        if hour != self.mLastUpdateHour:
            self.mLastUpdateHour = hour
            # If the hour changes, dump and clean out old bins
            self.flushOldBins(query.mStartTime)

    def queryResponse(self, query):
        "Update associated bin for the time specified, creating if necessary"
        # Hour and minute bins
        t = time.localtime(query.mStartTime)
        hour_bin_str = time.strftime("%Y-%m-%d-%H", t)
        minute_bin_str = time.strftime("%Y-%m-%d-%H-%M", t)
        hour = t[3]
        minute = t[4]
        # FIXME: These start times are a bit inaccurate, but should be fine under heavy query load.
        if not hour_bin_str in self.mHourBins:
            self.mHourBins[hour_bin_str] = LLQueryStatMap(hour_bin_str, query.mStartTime)
        if not minute_bin_str in self.mMinuteBins:
            self.mMinuteBins[minute_bin_str] = LLQueryStatMap(hour_bin_str, query.mStartTime)
            
        self.mHourBins[hour_bin_str].queryResponse(query)
        self.mMinuteBins[minute_bin_str].queryResponse(query)
        

# MySQL protocol sniffer, using tcpdump, ncap packet parsing and mysql internals
# http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol
class LLQueryStream:
    "Process a raw tcpdump stream (in raw libpcap format)"
    def __init__(self, in_file):
        self.mInFile = in_file
        self.mStartTime = time.time()

        #
        # A list of all outstanding "connections", and what they're doing.
        # This is necessary in order to get script timing and other information.
        #
        self.mConnStatus = {}
        self.mConnKeys = []
        self.mConnCleanupIndex = 0

        #
        # Parse/skip past the libpcap global header
        #
        
        #guint32 magic_number;   /* magic number */
        #guint16 version_major;  /* major version number */
        #guint16 version_minor;  /* minor version number */
        #gint32  thiszone;       /* GMT to local correction */
        #guint32 sigfigs;        /* accuracy of timestamps */
        #guint32 snaplen;        /* max length of captured packets, in octets */
        #guint32 network;        /* data link type */

        # Skip past the libpcap global header
        format = 'IHHiIII'
        size = struct.calcsize(format)
        header_bin = self.mInFile.read(size)
        res = struct.unpack(format, header_bin)

    def createConnection(self, client_ip_port, pkt_time):
        # Track the connection, create a new one or return existing
        if not client_ip_port in self.mConnStatus:
            self.mConnStatus[client_ip_port] = LLConnStatus(client_ip_port, pkt_time)
            # Track a new key that we need to garbage collect
            self.mConnKeys.append(client_ip_port)
        conn = self.mConnStatus[client_ip_port]
        return conn
    
    def closeConnection(self, ip_port):
        if ip_port in self.mConnStatus:
            del self.mConnStatus[ip_port]

    def cleanupConnection(self,cur_time):
        # Cleanup some number of stale connections.
        CONNECTION_EXPIRY=900.0
        if self.mConnCleanupIndex >= len(self.mConnKeys):
            self.mConnCleanupIndex = 0
            # Skip if no keys
            if len(self.mConnKeys) == 0:
                return
        key = self.mConnKeys[self.mConnCleanupIndex]
        if key in self.mConnStatus:
            # Clean up if it's too old
            if self.mConnStatus[key].mLastUpdate < (cur_time - CONNECTION_EXPIRY):
                del self.mConnStatus[key]
                #print "Cleaning up old key:", key
                #print "num conns:", len(self.mConnStatus)
                #print "num keys", len(self.mConnKeys)
        else:
            # Clean up if the connection is already removed
            del self.mConnKeys[self.mConnCleanupIndex]
        self.mConnCleanupIndex += 1

    def getNextEvent(self):
        # Get the next event out of the packet stream

        td_format = 'IIII'
        ip_format = '!BBHHHBBHII'
        tcp_format = '!HHIIBBHHH'
        while 1:
            #
            # Parse out an individual packet from the tcpdump stream
            #
            # Match the packet header

            # Pull a record (packet) off of the wire

            # Packet header
            # guint32 ts_sec;         /* timestamp seconds */
            # guint32 ts_usec;        /* timestamp microseconds */
            # guint32 incl_len;       /* number of octets of packet saved in file */
            # guint32 orig_len;       /* actual length of packet */
            ph_bin = self.mInFile.read(16)
            res = struct.unpack(td_format, ph_bin)
            ts_sec = res[0]
            ts_usec = res[1]
            pkt_time = ts_sec + (ts_usec/1000000.0)
            incl_len = res[2]
            orig_len = res[3]

            # Packet data (incl_len bytes)
            raw_data = self.mInFile.read(incl_len)

            # Parse out the MAC header
            # Don't bother, we don't care - 14 byte header
            mac_offset = 14

            # Parse out the IP header (min 20 bytes)
            # 4 bits - version
            # 4 bits - header length in 32 bit words
            # 1 byte - type of service
            # 2 bytes - total length
            # 2 bytes - fragment identification
            # 3 bits - flags
            # 13 bits - fragment offset
            # 1 byte - TTL
            # 1 byte - Protocol (should be 6)
            # 2 bytes - header checksum
            # 4 bytes - source IP
            # 4 bytes - dest IP
            
            ip_header = struct.unpack(ip_format, raw_data[mac_offset:mac_offset + 20])

            # Assume all packets are TCP
            #if ip_header[6] != 6:
            #    print "Not TCP!"
            #    continue
            
            src_ip_bin = ip_header[8]
            src_ip = lookup_ip_string(src_ip_bin)
            #src_ip = "%d.%d.%d.%d" % ((src_ip_bin & 0xff000000L) >> 24,
            #                          (src_ip_bin & 0x00ff0000L) >> 16,
            #                          (src_ip_bin & 0x0000ff00L) >> 8,
            #                          src_ip_bin & 0x000000ffL)
            dst_ip_bin = ip_header[9]
            dst_ip = lookup_ip_string(dst_ip_bin)
            #dst_ip = "%d.%d.%d.%d" % ((dst_ip_bin & 0xff000000L) >> 24,
            #                          (dst_ip_bin & 0x00ff0000L) >> 16,
            #                          (dst_ip_bin & 0x0000ff00L) >> 8,
            #                          dst_ip_bin & 0x000000ffL)
            
            ip_size = (ip_header[0] & 0x0f) * 4
            

            # Parse out the TCP packet header
            # 2 bytes - src_prt
            # 2 bytes - dst_port
            # 4 bytes - sequence number
            # 4 bytes - ack number
            # 4 bits - data offset (size in 32 bit words of header
            # 6 bits - reserved
            # 6 bits - control bits
            # 2 bytes - window
            # 2 bytes - checksum
            # 2 bytes - urgent pointer

            tcp_offset = mac_offset + ip_size
            tcp_header = struct.unpack(tcp_format, raw_data[tcp_offset:tcp_offset+20])
            tcp_size = ((tcp_header[4] & 0xf0) >> 4) * 4

            src_port = tcp_header[0]
            dst_port = tcp_header[1]

            # 3 bytes - packet length
            # 1 byte - packet number
            # 1 byte - command
            # <n bytes> - args
            pkt_offset = tcp_offset + tcp_size

            if len(raw_data) == pkt_offset:
                continue

            # Clearly not a mysql packet if it's less than 5 bytes of data
            if len(raw_data) - pkt_offset < 5:
                continue

            src_ip_port = "%s:%d" % (src_ip, src_port)
            dst_ip_port = "%s:%d" % (dst_ip, dst_port)

            if src_port == 3306:
                #
                # We are processing traffic from mysql server -> client
                # This primarily is used to time how long it takes for use
                # to start receiving data to the client from the server.
                #
                mysql_arr = array.array('B', raw_data[pkt_offset])
                result_type = ord(raw_data[pkt_offset])

                # Get or create connection
                conn = self.createConnection(dst_ip_port, pkt_time)

                # Update the status of this connection, including query times on
                # connections
                if conn.updateResponse(pkt_time, result_type):
                    # Event: Initial query response
                    return "QueryResponse", conn.mLastQuery
                continue
            if dst_port == 3306:
                #
                # Processing a packet from the client to the server
                #

                # HACK! This is an easy place to put this where we can get packet time that only happens once or so per event.
                # Garbage collect connections
                self.cleanupConnection(pkt_time)

                # Pull out packet length from the header
                mysql_arr = array.array('B', raw_data[pkt_offset:pkt_offset+5])
                pkt_len = mysql_arr[0] + (long(mysql_arr[1]) << 8) + (long(mysql_arr[2]) << 16)

                pkt_number = mysql_arr[3]

                # Find the connection associated with this packet
                
                # Get or create connection
                conn = self.createConnection(src_ip_port, pkt_time)

                #if conn.mLastMysqlPacketNumber != (pkt_number - 1):
                #    print "Prev:", conn.mLastMysqlPacketNumber, "Cur:", pkt_number
                conn.mLastMysqlPacketNumber = pkt_number
                
                cmd = mysql_arr[4]
                # If we're not a command, do stuff
                if cmd > 0x1c:
                    # Unfortunately, we can't trivially tell the difference between
                    # various non-command packets
                    # Assume that these are all AuthResponses for now.

                    conn.updateNonCommand(pkt_time, raw_data[pkt_offset:])
                    if "QuerySent" == conn.mCurState:
                        return ("QueryStart", conn.mLastQuery)
                    continue

                query = None

                if cmd == 1:
                    # Event: Quitting a connection
                    conn.quit(src_ip, src_port, pkt_time)
                    # This connection is closing, get rid of it
                    self.closeConnection(src_ip_port)
                    return ("Quit", conn.mLastQuery)
                elif cmd == 3:
                    # Event: Starting a query
                    conn.queryStart(src_ip, src_port, pkt_time, raw_data, pkt_len, pkt_offset + 5)

                    # Only return an QueryStart if we have the whole query
                    if "QuerySent" == conn.mCurState:
                        return ("QueryStart", conn.mLastQuery)
                else:
                    pass

IP_PORT_RE = re.compile("(\S+):(\d+)")
EVENT_RE = re.compile("(\S+)\t(\S+):(\d+)\t(\S+)\t(\S+)")
SECTION_RE = re.compile("\*{38}")


class LLLogQueryStream:
    "Process a query stream dump to generate a query stream class"
    "Process a raw tcpdump stream (in raw libpcap format)"
    def __init__(self, lineiter):
        self.mLineIter = lineiter
        self.mStartTime = None

        #
        # A list of all outstanding "connections", and what they're doing.
        # This is necessary in order to get script timing and other information.
        #
        self.mConnStatus = {}

    def closeConnection(self, ip_port):
        if ip_port in self.mConnStatus:
            del self.mConnStatus[ip_port]

    def getNextEvent(self):
        # Get the next event out of the file
        cur_event = None
        event_time = None
        event_type = None
        ip = None
        port = None
        ip_port = None
        cur_state = 'Metadata'
        for line in self.mLineIter:
            if line == '':
                return (None, None)
            if cur_state == 'Metadata':
                # We're looking for an event.  Actually we better find one.
                m = EVENT_RE.match(line)
                if not m:
                    #raise "Missing event on line: %s" % line
                    continue
                else:
                    event_time = float(m.group(1))
                    ip = m.group(2)
                    port = int(m.group(3))
                    ip_port = m.group(2)+":"+m.group(3)
                    clean_host = m.group(4)
                    event_type = m.group(5)
                    query_str = ''
                    cur_state = 'Query'
            elif cur_state == 'Query':
                if not SECTION_RE.match(line):
                    query_str += line
                else:
                    # We're done
                    # Generate the event to return
                    # Track the connection if we don't know about it yet.
                    conn = self.createConnection(ip_port, event_time)

                    if event_type == 'QueryStart':
                        conn.queryStartProcessed(ip, port, event_time, query_str)
                        return ("QueryStart", conn.mLastQuery)
                    elif event_type == 'QueryResponse':
                        # Update the status of this connection, including query times on
                        # connections
                        # Hack: Result type defaults to zero
                        if conn.updateResponse(event_time, 0):
                            # Event: Initial query response
                            return ("QueryResponse", conn.mLastQuery)
                        else:
                            # Skip responses which we don't have the start for
                            cur_state = 'Metadata'
                    elif event_type == 'Quit':
                        # Event: Quitting a connection
                        conn.quit(ip, port, event_time)
                        # This connection is closing, get rid of it
                        self.closeConnection(ip_port)
                        return ("Quit", conn.mLastQuery)
                    else:
                        raise ("Unknown event type %s" % event_type)
        return (None, None)

def start_dump(host, port):
    # Start up tcpdump pushing data into netcat on the sql server
    interface = "eth0"
    
    # Start up tcpdump pushing data into netcat on the sql server
    SRC_DUMP_CMD = "ssh root@%s '/usr/sbin/tcpdump -n -s 0 -w - -i %s dst port 3306 or src port 3306 | nc %s %d'" \
                   % (host, interface, socket.getfqdn(), port)
    os.popen2(SRC_DUMP_CMD, "r")

def remote_mysql_stream(host):
    # Create a server socket, then have tcpdump dump stuff to it.
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    bound = False
    port = 9999
    while not bound:
        try:
            serversocket.bind((socket.gethostname(), port))
            bound = True
        except:
            print port, " already bound, trying again"
            port += 1
    print "Bound port %d" % port
    serversocket.listen(1)

    # Fork off the dumper, start the server on the main connection
    pid = os.fork()
    if not pid:
        # Child process which gets data from the database
        time.sleep(1.0)
        print "Starting dump!"
        start_dump(host, port)
        print "Exiting dump!"
        sys.exit(0)

    print "Starting server"
    (clientsocket, address) = serversocket.accept()
    print "Accepted connection", address

    # Start listening to the data stream
    return clientsocket.makefile("rb")

#
# Utility stuff for query cleaner
#
# This is a Python port of (part of) the fingerprint() function from
# the mk-query-digest script in Maatkit, added by Yoz, with various additions/tweaks

hex_wildcard = r"[0-9a-f]"
word = hex_wildcard + r"{4}-"
long_word = hex_wildcard + r"{8}-"
very_long_word = hex_wildcard + r"{12}"
UUID_REGEX_STRING = long_word + word + word + word + very_long_word

hex_re = re.compile("^[\da-f]+$",re.I)
uuid_re = re.compile("^"+UUID_REGEX_STRING+"$",re.I)

def string_replace(match):
    "Called by string-matching regexp in replacers"
    if uuid_re.match(match.group(1)):
        return "*uuid*"
    return "*string*"
    

# list of (match,replacement) tuples used by clean_query()
replacers = [
    # Disabling comment removal because we may put useful inspection info in there
    #(re.compile(r'(?:--|#)[^\'"\r\n]*(?=[\r\n]|\Z)',re.I),""), # one-line comments
    #(re.compile(r"/\*[^!].*?\*/",re.I|re.M|re.S),""), # But not /*!version */

    (re.compile(r"\\\\"),""), # remove backslash pairs that may confuse the next line    
    (re.compile(r"\\[\"']"),""), # remove escaped quotes
    
    (re.compile(r'"([^"]*)"',re.I),string_replace), # quoted strings
    (re.compile(r"'([^']*)'",re.I),string_replace), # quoted strings
    
    # this next one may need more work, due to "UPDATE ... SET money = money-23"
    # the next two are significantly different from the maatkit original code
    (re.compile(r"(?<![\w\)\d])(\s*)\-\d+(\.\d+)?",re.I),"*num*"), # negative reals
    (re.compile(r"(?<![\w])\d+(\.\d+)?",re.I),"*num*"), # positive reals
    # mk-query-digest has s/[xb.+-]\?/?/g; as "clean up leftovers" here, whatever that means - I've left it out
    
    (re.compile(r"^\s+",re.I),""), # chop off leading whitespace
    (re.compile(r"\s+$",re.I|re.M|re.S),""), # kill trailing whitespace
    
    # reduce IN and VALUES lists (look for previously-cleaned placeholders)
    (re.compile(r"\b(in|values)(?:[\s,]*\(([\s\,]*\*(num|string|uuid)\*)*[\s,]*\))+",
                re.I|re.X),"\\1(*values*)"), # collapse IN and VALUES lists
    
    # This next one collapses chains of UNIONed functionally-identical queries,
    # but it's only really useful if you're regularly seeing more than 2 queries
    # in a chain. We don't seem to have any like that, so I'm disabling this.
    #(re.compile(r"\b(select\s.*?)(?:(\sunion(?:\sall)?)\s\1)+",re.I),"\\1 -- repeat\\2 --"), # collapse UNION
    
    # remove "OFFSET *num*" when following a LIMIT
    (re.compile(r"\blimit \*num\*(?:, ?\*num\*| offset \*num\*)?",re.I),"LIMIT *num*")
]

prepare_re = re.compile('PREPARE.*', re.IGNORECASE)
deallocate_re = re.compile('DEALLOCATE\s+PREPARE.*', re.IGNORECASE)
execute_re = re.compile('EXECUTE.*', re.IGNORECASE)
mdb_re = re.compile('MDB2_STATEMENT\S+')

def clean_query(query, num_words):
    "Generalizes a query by removing all unique information"
    # Strip carriage returns
    query = query.replace("\n", " ")

    # Screw it, if it's a prepared statement or an execute, generalize the statement name
    if prepare_re.match(query):
        query = mdb_re.sub('*statement*', query)
        return query
    if execute_re.match(query):
        query = mdb_re.sub('*statement*', query)
    if deallocate_re.match(query):
        query = "DEALLOCATE PREPARE"
        return query

    # Loop through the replacers and perform each one
    for (replacer, subst) in replacers:
        # try block is here because, apparently, string_re may throw an exception
        # TODO: investigate the above
        try:
            query = replacer.sub(subst, query)
        except:
            pass

    # After we do the cleanup, then we get rid of extra whitespace
    words = query.split(None)
    query = " ".join(words)    
    return query

def test_clean_query(query):
    "A debug version of the query cleaner which prints steps as it goes"

    # Strip carriage returns
    query = query.replace("\n", " ")

    # Screw it, if it's a prepared statement or an execute, generalize the statement name
    if prepare_re.match(query):
        query = mdb_re.sub('*statement*', query)
        return query
    if execute_re.match(query):
        query = mdb_re.sub('*statement*', query)
    if deallocate_re.match(query):
        query = "DEALLOCATE PREPARE"
        return query

    # Loop through the replacers and perform each one
    for (replacer, subst) in replacers:
        try:
            if replacer.search(query) == None:
                print replacer.pattern," : No match"
            else:
                query = replacer.sub(subst, query)
                print replacer.pattern," : ",query
        except:
            pass

    # After we do the cleanup, then we get rid of extra whitespace
    words = query.split(None)
    query = " ".join(words)    
    return query


#
# Hostname cache - basically, caches the "linden" host type for a particular IP address
# or hostname
#
sim_re = re.compile(".*sim\d+.*")
web_re = re.compile("int\.web\d+.*")
iweb_re = re.compile("int\.iweb\d+.*")
webds_re = re.compile(".*web-ds\d+.*")
webster_re = re.compile(".*webster\d+.*")
bankds_re = re.compile(".*bank-ds\d+.*")
xmlrpc_re = re.compile(".*xmlrpc\d+.*")
login_re = re.compile(".*login\d+.*")
data_re = re.compile(".*data\..*")
#xmlrpc_re = re.compile("(?:int\.omgiwanna.*)|(?:int\.pony.*)")
ip_re = re.compile("\d+\.\d+\.\d+\.\d+")
ll_re = re.compile("(.*)\.lindenlab\.com")

host_type_cache = {}
def get_host_type(host):
    "Returns the genericized linden host type from an IP address or hostname"
#    if host in host_type_cache:
#        return host_type_cache[host]

    named_host = str(host)
    if ip_re.match(host):
        # Look up the hostname
        try:
            named_host = str(socket.gethostbyaddr(host)[0])
        except:
            pass

    # Figure out generic host type
    host_type = named_host
    if sim_re.match(named_host):
        host_type = "sim"
    elif login_re.match(named_host):
        host_type = "login"
    elif webster_re.match(named_host):
        host_type = "webster"
    elif bankds_re.match(named_host):
        host_type = "bank-ds"
    elif web_re.match(named_host):
        host_type = "web"
    elif iweb_re.match(named_host):
        host_type = "iweb"
    elif webds_re.match(named_host):
        host_type = "web-ds"
    elif data_re.match(named_host):
        host_type = "data"
    elif xmlrpc_re.match(named_host):
        host_type = "xmlrpc"
    m = ll_re.match(host_type)
    if m:
        host_type = m.group(1)
    host_type_cache[host] = host_type
    return (host_type, named_host)


def LLLogIter(filenames):
    "An iterator that iterates line by line over a series of files, even if they're compressed."
    for f in filenames:
        curr = open_log_file(f)
        for line in curr:
            yield line

            
def open_log_file(filename):
    # Open the logfile (even if it's compressed)
    if re.compile(".+\.gz").match(filename):
        # gzipped file, return a gzipped file opject
        return gzip.open(filename,"r")
    else:
        return open(filename, "r")

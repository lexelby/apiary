#!/usr/bin/python
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

"""
Log all queries hitting a particular mysql database
"""

try:
    import psyco
    psyco.full()
except:
    pass

import array
import curses
import curses.wrapper
import getopt
import os.path
import re
import socket
import struct
import sys
import time
import math

LOG_ROTATION_INTERVAL=3600
MAX_LOGS = 36
MIN_BIN=-15
MAX_BIN=10
ip_table = {}
host_type_cache = {}

sim_re = re.compile(".*sim\d+.*")
web_re = re.compile("int\.web\d+.*")
iweb_re = re.compile("int\.iweb\d+.*")
webds_re = re.compile(".*web-ds\d+.*")
login_re = re.compile(".*login\d+.*")
data_re = re.compile(".*data\..*")
xmlrpc_re = re.compile("(?:int\.omgiwanna.*)|(?:int\.pony.*)")
ip_re = re.compile("\d+\.\d+\.\d+\.\d+")
ll_re = re.compile("(.*)\.lindenlab\.com")

#
# Utility stuff for query cleaner
#

hex_wildcard = r"[0-9a-fA-F]"
word = hex_wildcard + r"{4,4}-"
long_word = hex_wildcard + r"{8,8}-"
very_long_word = hex_wildcard + r"{12,12}"
UUID_REGEX_STRING = long_word + word + word + word + very_long_word
uuid_re = re.compile("[\"\']"+UUID_REGEX_STRING+"[\"\']")
hex_re = re.compile("[\"\'][\da-f]+[\"\']")
num_re = re.compile("[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?")

# Quoted string re from: http://blog.stevenlevithan.com/archives/match-quoted-string
string_re = re.compile(r'([\"\'])(?:(?=(\\?))\2.)*?\1')

values_re = re.compile('VALUES\s+\(.*\)', re.IGNORECASE)
in_re = re.compile('IN\s+\(.*\)', re.IGNORECASE)

prepare_re = re.compile('PREPARE.*', re.IGNORECASE)
deallocate_re = re.compile('DEALLOCATE\s+PREPARE.*', re.IGNORECASE)
execute_re = re.compile('EXECUTE.*', re.IGNORECASE)
mdb_re = re.compile('MDB2_STATEMENT\S+')
 

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

    def closeConnection(self, ip_port):
        if ip_port in self.mConnStatus:
            del self.mConnStatus[ip_port]


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

                # Track the connection if we don't know about it yet.
                if not dst_ip_port in self.mConnStatus:
                    self.mConnStatus[dst_ip_port] = LLConnStatus(dst_ip_port, pkt_time)
                conn = self.mConnStatus[dst_ip_port]

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

                # Pull out packet length from the header
                mysql_arr = array.array('B', raw_data[pkt_offset:pkt_offset+5])
                pkt_len = mysql_arr[0] + (long(mysql_arr[1]) << 8) + (long(mysql_arr[2]) << 16)

                pkt_number = mysql_arr[3]

                # Find the connection associated with this packet
                if not src_ip_port in self.mConnStatus:
                    self.mConnStatus[src_ip_port] = LLConnStatus(src_ip_port, pkt_time)
                conn = self.mConnStatus[src_ip_port]

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

class LLQuery:
    fromLLSDStats = staticmethod(llquery_from_llsd)
    def __init__(self, host, port, query, start_time):
        # Store information which will be serialized for metadata in a map
        self.mData = {}
        self.mData['host'] = host
        self.mData['port'] = port
        self.mData['query'] = query

        # Metadata
        self.mData['host_clean'] = None
        self.mData['query_clean'] = None
        self.mData['tables'] = []

        # Stats information
        self.mNumQueries = 0
        self.mTotalTime = 0.0
        self.mOutQueries = 0
        self.mTotalTimeCorrected = 0.0
        self.mNumQueriesCorrected = 0
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
            self.mData['host_clean'] = host_type(self.mData['host'])
            self.mData['query_clean'] = clean_query(self.mData['query'], 0)

    def getAvgTimeCorrected(self):
        return self.mTotalTimeCorrected/self.mNumQueriesCorrected

    def queryStart(self):
        "When collecting query stats, use this when the query is receieved"
        self.mNumQueries += 1
        self.mOutQueries += 1

    def queryResponse(self, elapsed):
        "When collecting stats, use this when the response is received"
        self.mTotalTime += elapsed
        self.mOutQueries -=1
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
        # Outliers are 3 orders of magnitude less than the total count
        if not self.mNumQueries:
            # FIXME: This is a hack because we don't save this information in the query count dump
            return
        min_queries = self.mNumQueries/1000
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

    sReadRE = re.compile("(SELECT.*)|(USE.*)", re.IGNORECASE)
    sSelectWhereRE = re.compile("\(?\s*?SELECT.+?FROM\s+\(?(.*?)\)?\s+WHERE.*", re.IGNORECASE)
    sSelectRE = re.compile("\(?\s*?SELECT.+?FROM\s+(.+)(?:\s+LIMIT.*|.*)", re.IGNORECASE)
    sUpdateRE = re.compile("UPDATE\s+(.+?)\s+SET.*", re.IGNORECASE)
    sReplaceRE = re.compile("REPLACE INTO\s+(.+?)(?:\s*\(|\s+SET).*", re.IGNORECASE)
    sInsertRE = re.compile("INSERT.+?INTO\s+(.+?)(?:\s*\(|\s+SET).*", re.IGNORECASE)
    sDeleteRE = re.compile("DELETE.+?FROM\s+(.+?)\s+WHERE.*", re.IGNORECASE)
    def analyze(self):
        "Does some query analysis on the query"
        if 'type' in self.mData:
            # Already analyzed
            return
        query = self.mData['query_clean']
        if LLQuery.sReadRE.match(query):
            self.mData['type'] = 'read'
        else:
            self.mData['type'] = 'write'

        self.mData['tables'] = get_query_tables(query)

    def dumpLine(self, elapsed, query_len = 0):
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
    
def clean_query(query, num_words):
    "Generalizes a query by removing all unique information"
    # Generalize the query, remove all unique information


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

    # Replace all "unique" information - strings, uuids, numbers
    query = uuid_re.sub("*uuid*", query)
    query = hex_re.sub("*hex*", query)
    try:
        query = string_re.sub("*string*", query)
    except:
        pass
    query = num_re.sub("*num*", query)

    # Get rid of all "VALUES ()" data.
    query = values_re.sub("VALUES (*values*)", query)
    # Get rid of all "IN ()" data.
    query = in_re.sub("IN (*values*)", query)
    # After we do the cleanup, then we get rid of extra whitespace
    words = query.split(None)
    query = " ".join(words)    
    return query


def host_type(host):
    "Returns the genericized linden host type from an IP address or hostname"
    if host in host_type_cache:
        return host_type_cache[host]

    named_host = host
    if ip_re.match(host):
        # Look up the hostname
        try:
            named_host = socket.gethostbyaddr(host)[0]
        except:
            pass

    # Figure out generic host type
    host_type = named_host
    if sim_re.match(named_host):
        host_type = "sim"
    elif login_re.match(named_host):
        host_type = "login"
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
    return host_type


def start_dump(host, port):
    # Start up tcpdump pushing data into netcat on the sql server
    interface = "eth0"
    
    # Start up tcpdump pushing data into netcat on the sql server
    SRC_DUMP_CMD = "ssh root@%s '/usr/sbin/tcpdump -n -s 0 -w - -i %s dst port 3306 or src port 3306 | nc %s %d'" \
                   % (host, interface, socket.getfqdn(), port)
    os.popen2(SRC_DUMP_CMD, "r")

def lookup_ip_string(ip_bin):
    if not ip_bin in ip_table:
        ip_table[ip_bin] = "%d.%d.%d.%d" % ((ip_bin & 0xff000000L) >> 24,
                                            (ip_bin & 0x00ff0000L) >> 16,
                                            (ip_bin & 0x0000ff00L) >> 8,
                                            ip_bin & 0x000000ffL)
    return ip_table[ip_bin]


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


def rotate_logs(log_path, query_log_file):
    # Fork to do the actual rotation/compression
    print "Rotating query logs"
    if query_log_file:
        query_log_file.close()
    need_gzip = False

    if os.path.exists(log_path+"/query.log"):
        os.rename(log_path+"/query.log", log_path+"/query.log.tmp")
        need_gzip = True
    
    query_log_file = open("%s/query.log" % log_path, "w")

    pid = os.fork()
    if pid:
        return query_log_file

    # Child process actually does the log rotation
    # Delete the oldest
    log_filename = log_path+"/query.log.%d.gz" % (MAX_LOGS)
    if os.path.exists(log_filename):
        os.remove(log_filename)

    for i in range(0, MAX_LOGS):
        # Count down from the max and rename
        n = MAX_LOGS - i
        log_filename = log_path+"/query.log.%d.gz" % n
        if os.path.exists(log_filename):
            os.rename(log_path + ("/query.log.%d.gz" % n), log_path + ("/query.log.%d.gz" % (n+1)))

    if need_gzip:
        # Compress the "first" log (query.log.tmp)
        os.rename(log_path + "/query.log.tmp", log_path + "/query.log.1")
        os.system('gzip -f %s' % (log_path + "/query.log.1"))
    print "Done rotating logs!"
    sys.exit(0)


def watch_host(query_stream, host):
    "Watches query traffic for a particular host.  Returns the overall query counts when exited by breaking"

    # Make output path
    output_path = "./%s" % host
    os.system("mkdir -p %s" % output_path)
    query_log_file = rotate_logs(output_path, None)

    last_log_time = time.time()

    done = False
    count = 0
    try:
        while not done:
            (event_type, query) = query_stream.getNextEvent()

            # Use the start time to determine which hour bin to put the query into
            start_time = query.mStartTime
            start_hour = time.localtime(start_time)[3]
            
            if event_type == "QueryStart":
                query_log_file.write("%f\t%s:%d\t%s\tQueryStart\n" % (query.mStartTime, query.mData['host'], query.mData['port'], query.mData['host_clean']))
                query_log_file.write("%s\n" % (query.mData['query']))
                query_log_file.write("**************************************\n")
                count += 1
            elif (event_type == "QueryResponse"):
                query_log_file.write("%f\t%s:%d\t%s\tQueryResponse\n" % (query.mResponseTime, query.mData['host'], query.mData['port'], query.mData['host_clean']))
                query_log_file.write("%s\n" % (query.mData['query']))
                query_log_file.write("**************************************\n")
            elif event_type == "Quit":
                # Quit is an "instantaneous" query, both start and response
                query_log_file.write("%f\t%s:%d\t%s\tQuit\n" % (query.mStartTime, query.mData['host'], query.mData['port'], query.mData['host_clean']))
                query_log_file.write("%s\n" % (query.mData['query']))
                query_log_file.write("**************************************\n")
                continue
            if not (count % 1000):
                try:
                    os.waitpid(-1, os.WNOHANG)
                except OSError:
                    pass
                if (time.time() - last_log_time) > LOG_ROTATION_INTERVAL:
                    last_log_time = time.time()
                    query_log_file = rotate_logs(output_path, query_log_file)

            
    except KeyboardInterrupt:
        pass
    query_log_file.close()


if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], "", ["host="])

    host = None
    for o, a in opts:
        if o in ("--host"):
            host = a
    if not host:
        print "Specify a host using --host="
        sys.exit(1)

    # Start up the stream from the target host and create a file
    # that we can hand to LLQueryStream
    query_stream_file = remote_mysql_stream(host)
    query_stream = LLQueryStream(query_stream_file)

    watch_host(query_stream, host)

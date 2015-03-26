#
# Copyright (c) 2014, DeviantArt Inc.
#


'''
This module implements a simple HTTP client with keepalive but not pipelining.
'''

import re
import socket
import sys
import os
import apiary
import optparse
import time
from httplib import HTTPResponse, IncompleteRead
import threading


class HTTPWorkerBee(apiary.WorkerBee):
    """A WorkerBee that sends HTTP Requests"""

    def __init__(self, options, *args, **kwargs):
        super(HTTPWorkerBee, self).__init__(options, *args, **kwargs)

        self.http_host = socket.gethostbyname(options.http_host)

        self.options = options
        self.connection = None

    def run(self):
        if self.options.http_stats_dir:
            self.stats = True
            self.stats_file = open("%s/%s-%s.log" % (self.options.http_stats_dir, os.getpid(), threading.current_thread().name), "w")
        else:
            self.stats = False

        super(HTTPWorkerBee, self).run()

    def start_job(self, job_id):
        try:
            self.connection = socket.socket()
            self.connection.settimeout(self.options.http_timeout)
            self.connection.connect((self.http_host, self.options.http_port))
        except Exception, e:
            self.error("error while connecting: %s" % e)
            self.connection = None

    def send_request(self, request):
        if self.connection:
            # tally request method
            #self.tally(request.split(" ", 1)[0])

            try:
                if self.stats:
                    start_time = time.time()

                self.connection.sendall(request)

                response = HTTPResponse(self.connection)
                response.begin()

                self.tally(response.status)

                if response.will_close:
                    # We hope this won't happen, but deal with it if it does.
                    self.connection.close()
                    self.connection = None

                while response.read():
                    pass

                if self.stats:
                    print >> self.stats_file, start_time, time.time() - start_time

                return True
            except IncompleteRead:
                self.error("error while reading response: IncompleteRead (terminating job)")

                if self.connection:
                    self.connection.close()
                    self.connection = None

            except Exception, e:  # TODO: more restrictive error catching?
                self.error("error while sending request and reading response: %s" % e)

                if self.connection:
                    self.connection.close()
                    self.connection = None

        return False

    def finish_job(self, job_id):
        if self.stats:
            self.stats_file.flush()

        if self.connection:
            try:
                self.connection.close()
            except:
                pass

        self.connection = None

WorkerBee = HTTPWorkerBee


def add_options(parser):
    g = optparse.OptionGroup(parser, 'HTTP options (--protocol http)')
    g.add_option('--http-host',
                      default="localhost", metavar='HOST',
                      help='HTTP server to connect to (default: %default)')
    g.add_option('--http-port',
                      default=80, type='int', metavar='PORT',
                      help='HTTP to connect to (default: %default)')
    g.add_option('--http-timeout',
                      default=10, type='int', metavar='SECONDS',
                      help='Timeout for HTTP socket operations (default: %default)')
    g.add_option('--http-read-size',
                      default=1024, type='int', metavar='BYTES',
                      help='Chunk size when reading (and discarding) response body (default: %default)')
    g.add_option('--http-stats-dir', metavar='DIR', help="where to store request time information (default: don't)")

    parser.add_option_group(g)

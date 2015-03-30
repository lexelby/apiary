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

        if options.http_stats_dir:
            try:
                os.mkdir(options.http_stats_dir)
            except OSError, err:
                if err.errno == 17:  # file exists
                    pass
                else:
                    raise

    def run(self):
        if self.options.http_stats_dir:
            self.stats = True
            self.stats_file = open("%s/%s-%s.log" % (self.options.http_stats_dir, os.getpid(), threading.current_thread().name), "w")
        else:
            self.stats = False

        super(HTTPWorkerBee, self).run()

    def _connect(self):
        try:
            self.connection = socket.socket()
            self.connection.settimeout(self.options.http_timeout)
            self.connection.connect((self.http_host, self.options.http_port))
        except Exception, e:
            self.error("error while connecting: %s" % e)
            self.connection = None

    def _disconnect(self):
        if self.connection:
            try:
                self.connection.close()
            except:
                pass

        self.connection = None

    def start_job(self, job_id):
        self.current_job_id = job_id
        self.request_num = -1

        self._connect()

    def send_request(self, request):
        self.request_num += 1

        if not self.connection:
            self._connect()

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

                while response.read():
                    pass

                if response.will_close:
                    # We hope our Connection: keep-alive won't be ignored, but
                    # deal with it if it does.
                    self._disconnect()

                if self.options.speedup < 0.8:
                    # if we're slowing down by too much, keep-alive will just
                    # result in the server getting bored between requests and
                    # dropping the connection, so disable it.
                    self._disconnect()

                if self.stats:
                    print >> self.stats_file, start_time, time.time() - start_time, self.current_job_id, self.request_num

                return True
            except IncompleteRead:
                self.error("error while reading response: IncompleteRead (terminating job)")
                self._disconnect()

            except Exception, e:  # TODO: more restrictive error catching?
                self.error("error while sending request and reading response: %s %s" % (type(e), e))
                self._disconnect()

                if self.connection:
                    self.connection.close()
                    self.connection = None

        # we want to keep trying in the face of errrors
        return True

    def finish_job(self, job_id):
        if self.stats:
            self.stats_file.flush()

        self._disconnect()

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

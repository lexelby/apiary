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


content_length_re = re.compile('content-length:\s+([0-9]+)\r\n', re.I | re.S)


class HTTPWorkerBee(apiary.WorkerBee):
    """A WorkerBee that sends HTTP Requests"""

    def __init__(self, options, *args, **kwargs):
        super(HTTPWorkerBee, self).__init__(options, *args, **kwargs)

        self.http_host = socket.gethostbyname(options.http_host)

        self.options = options
        self.connection = None

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

        # Sanity check: if we're sending a request with a content-length but
        # we don't have that many bytes to send, we'll just get a 504.  Don't
        # send it and instead report a client error.

        parts = request.split('\r\n\r\n', 1)

        if len(parts) > 1:
            req, body = parts

            match = content_length_re.search(req)
            if match:
                if len(body) < int(match.group(1)):
                    self.error("request body of incorrect size")

                    return True

        if not self.connection:
            self._connect()

        if self.connection:
            # tally request method
            #self.tally(request.split(" ", 1)[0])

            try:
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

    parser.add_option_group(g)

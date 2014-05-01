#
# Copyright (c) 2014, DeviantArt Inc.
#


'''
This module implements the simple protocol used by CountDB at DeviantArt.
'''

import re
import socket
import sys
import apiary
import optparse

class CountDBWorkerBee(apiary.WorkerBee):
    """A WorkerBee that sends requests to CountDB"""

    def __init__(self, options):
        super(CountDBWorkerBee, self).__init__()

        self.options = options
        self.connection = None

    def start_job(self, job_id):
        socket.setdefaulttimeout(self.options.countdb_timeout)

        try:
            self.connection = socket.create_connection(
                (self.options.countdb_host, self.options.countdb_port))
        except Exception, e:
            self.error(str(e))
            self.connection = None

    def send_request(self, request):
        if self.connection:
            try:
                self.connection.sendall("json %s\0" % request)
                self.connection.recv(self.options.countdb_recv_size)
                return True
            except Exception, e:  # TODO: more restrictive error catching?
                self.error("%s" % e)

        return False

    def finish_job(self, job_id):
        if self.connection:
            try:
                self.connection.close()
            except:
                pass

        self.connection = None

WorkerBee = CountDBWorkerBee


def add_options(parser):
    g = optparse.OptionGroup(parser, 'CountDB options (--protocol countdb)')
    g.add_option('--countdb-host',
                      default="localhost", metavar='HOST',
                      help='CountDB server to connect to (default: %default)')
    g.add_option('--countdb-port',
                      default=3939, type='int', metavar='PORT',
                      help='CountDB port to connect to (default: %default)')
    g.add_option('--countdb-timeout',
                      default=10, type='int', metavar='SECONDS',
                      help='Timeout for countdb socket operations (default: %default)')
    g.add_option('--countdb-recv-size',
                      default=1024, type='int', metavar='BYTES',
                      help='Maximum number of bytes to wait for in response to each request (default: %default)')

    parser.add_option_group(g)

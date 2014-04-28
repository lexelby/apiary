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

'''
This module contains an abstracted interface to RabbitMQ for producers and
consumers.
'''

import sys
import socket
import amqplib.client_0_8 as amqp

from debug import debug, traced_func, traced_method


class ConnectionError(Exception):
    pass


amqp_host = 'localhost'
amqp_userid = 'apiary'
amqp_password = 'beehonest'
amqp_vhost = '/apiary'
amqp_exchange = 'b.direct'


class Transport(object):
    """A simple message queue-like transport system

    Built on AMQP, hides much of the details of that interface and presents
    a simple set of utilities for sending and receiving messages on named
    queues.

    """

    def __init__(self, options=None):
        self._amqp_host = getattr(options, 'amqp_host', amqp_host)
        self._amqp_vhost = getattr(options, 'amqp_vhost', amqp_vhost)
        self._amqp_userid = getattr(options, 'amqp_userid', amqp_userid)
        self._amqp_password = getattr(options, 'amqp_password', amqp_password)
        self._verbose = getattr(options, 'verbose', False)
        self._no_ack = True

    def _server_connect(self):
        try:
            if self._verbose >= 2:
                print "connecting to amqp '%s@%s%s'" % (self._amqp_userid, self._amqp_host, self._amqp_vhost)
            self._conn = amqp.Connection(
                    self._amqp_host, virtual_host=self._amqp_vhost,
                    userid=self._amqp_userid, password=self._amqp_password)
        except socket.error, e:
            raise ConnectionError("Error connecting to '%s': %s" % (self._amqp_host, e))

        self._ch = self._conn.channel()
        self._ch.access_request('/data', active=True, write=True, read=True)
        self._ch.exchange_declare(amqp_exchange, 'direct', durable=False, auto_delete=False)

    def _server_close(self):
        try:
            self._ch.close()
            self._ch = None
        except:
            pass
        try:
            self._conn.close()
            self._conn = None
        except:
            pass

    def connect(self):
        self._server_connect()
        self._queues = []

    def close(self):
        for qname in self._queues:
            self._ch.queue_delete(qname)
        self._queues = []
        self._server_close()

    def queue(self, queue='', inControl=True, clean=False):
        queue, _, _ = self._ch.queue_declare(queue, durable=False, auto_delete=False)
        try:
            self._ch.queue_bind(queue, amqp_exchange, queue)
        except amqp.AMQPChannelException, e:
            sys.exit("Error binding to queue: %s" % e[1])
        if inControl:
            self._queues.append(queue)
        if clean:
        # we purge the queues when we first initialize them
            if self._verbose >= 2:
                print "purging queue " + queue
            self._ch.queue_purge(queue)
        return queue

    def set_prefetch(self, num_messages):
        self._no_ack = num_messages == 0
        self._ch.basic_qos(prefetch_size=0, prefetch_count=num_messages, a_global=False)

    # same as queue(), only without inControl, so it consumes instead of appending
    def usequeue(self, queue, clean=False):
        self.queue(queue, inControl=False, clean=clean)

    @traced_method
    def send(self, queue, data):
        msg = amqp.Message(data)
        self._ch.basic_publish(msg, amqp_exchange, queue)

    def consume(self, queue, tag, fn, exclusive=True):
        fn = traced_func(fn)
        return self._ch.basic_consume(
            queue,
            tag,
            no_ack=self._no_ack,
            exclusive=exclusive,
            callback=fn)

    def cancelconsume(self, tag):
        self._ch.basic_cancel(tag)

    def wait(self):
        while self._ch.callbacks:
            self._ch.wait()

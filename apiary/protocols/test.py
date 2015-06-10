#
# Copyright (c) 2014, DeviantArt Inc.
#


'''
This module implements a dummy protocol for testing the rest of apiary.
'''

import sys
import time
import optparse
from random import random, randint
import apiary

class TestWorkerBee(apiary.WorkerBee):
    """A WorkerBee that sends requests to CountDB"""

    def __init__(self, options, *args, **kwargs):
        super(TestWorkerBee, self).__init__(options, *args, **kwargs)

        self.options = options
        self.duration_range = options.max_duration - options.min_duration

    def send_request(self, request):
        time.sleep(self.options.min_duration + random() * self.duration_range)

        if random() < self.options.error_probability:
            self.error("error %s" % randint(1, 5))

        return False

WorkerBee = TestWorkerBee


def add_options(parser):
    g = optparse.OptionGroup(parser, 'Test protocol options (--protocol test)')
    g.add_option('--min-duration',
                      default=0.01, metavar='SECONDS',
                      help='Minimum length of a request (default: %default)')
    g.add_option('--max-duration',
                      default=1.2, metavar='SECONDS',
                      help='Minimum length of a request (default: %default)')
    g.add_option('--error-probability',
                      default=0.01, type=float,
                      help='Log an error with this probability (default: %default)')

    parser.add_option_group(g)

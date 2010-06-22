#!/usr/bin/env python
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
This is the main script for apiary.  It is responsible for all things
related to configuration, option parsing, and process/thread management.
'''

import optparse
import sys
import os
import threading
import thread

import apiary
import apiary.tools.debug
from apiary.tools.debug import *
import apiary.mysql
import apiary.http


protocols = {
    'mysql': apiary.mysql,
    'http': apiary.http,
    }


def main(args=sys.argv[1:]):
    options, arguments = parse_args(args)
    if options.debug:
        apiary.tools.debug.enable_debug()

    protomod = protocols[options.protocol]

    if options.clean:
        apiary.clean(options)
            
    if (not options.clean and not options.queenbee and options.fork == 0):
        sys.exit('Nothing to do: specify one or more of --queenbee, --fork or --clean')
    
    if options.profile:
        from apiary.tools import lsprof
        profiler = lsprof.Profiler()
        profiler.enable(subcalls=True)

    if options.fork > 0:
        if not options.queenbee:
            options.fork -= 1 # Hack: this process will be one of the workers.
        start_forks(options, arguments)
        # *HACK: start_forks sets this option False inside children,
        # so this means "if we are a child process or --queenbee was
        # not passed":
        if not options.queenbee:
            run_worker(protomod.workerbee_cls, options, arguments)

    if options.queenbee:
        run_queenbee(protomod.queenbee_cls, options, arguments)
        
    if options.profile:
        profiler.disable()
        stats = lsprof.Stats(profiler.getstats())
        stats.sort()
        stats.pprint(top=10, file=sys.stderr, climit=5)

    
def start_forks(options, arguments):
    debug('Starting %d worker processes.', options.fork)
    
    if os.fork() == 0:
        # now in child
        os.setsid() # magic that creates a new process group
        options.queenbee = False # ensure forks don't run queenbee
        for i in xrange(0, options.fork):
            if os.fork() == 0:
                # now in grandchild
                return # escape loop, keep processing
        sys.exit(0)
    else:
        options.workers = 0 # ensure parent doesn't run workers

def run_worker(worker_cls, options, arguments):
    w = worker_cls(options, arguments)
    w.main()
        
def run_queenbee(queenbee_cls, options, arguments):
    debug('Launching %r; options %r; arguments %r', queenbee_cls, options, arguments)
    
    c = queenbee_cls(options, arguments)
    c.main()


def parse_args(args = []):
    parser = build_option_parser()
    options, args = parser.parse_args(args)
    
    if not options.protocol:
        parser.error("--protocol is required")

    return options, args
    

def build_option_parser():
    parser = optparse.OptionParser()
    parser.add_option('--protocol', dest='protocol', 
                      type='choice', choices=protocols.keys(),
                      help='Protocol of queries to be replayed.  Supported protocols: %s' % ', '.join(protocols.keys()))
    
    apiary.add_options(parser)

    for proto, mod in protocols.items():
        g = optparse.OptionGroup(parser, 'Options for %s protocol' % (proto,))
        mod.add_options(g)
        parser.add_option_group(g)

    return parser


    

if __name__ == '__main__':
    main()

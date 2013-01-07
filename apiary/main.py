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
import multiprocessing
import signal
import time

import apiary
import apiary.tools.debug
from apiary.tools.debug import *
import apiary.mysql
import apiary.http


protocols = {
    'mysql': apiary.mysql,
    'http': apiary.http,
    }

class SignalReceived(Exception):
    pass

def handle_signal(signum, frame):
    raise SignalReceived()


def main(args=sys.argv[1:]):
    options, arguments = parse_args(args)
    if options.debug:
        apiary.tools.debug.enable_debug()

    protomod = protocols[options.protocol]

    if options.clean:
        apiary.clean(options)
            
    if (not options.clean and not options.queenbee and options.workers == 0):
        sys.exit('Nothing to do: specify one or more of --queenbee, --fork or --clean')

    if options.workers and options.queenbee:
        sys.exit('Cannot specify both --queenbee and --workers')
    
    if options.profile:
        from apiary.tools import lsprof
        profiler = lsprof.Profiler()
        profiler.enable(subcalls=True)

        
    if options.workers:
        
        workers = []
        
        for i in xrange(options.workers):
            process = multiprocessing.Process(target=run_worker, args=[protomod.workerbee_cls, options, arguments])
            
            if not options.background:
                process.daemon = True
            process.start()
            workers.append(process)
        
        if not options.background:
            run_monitor(workers)
    elif options.queenbee:
        run_queenbee(protomod.queenbee_cls, options, arguments)
        
    if options.profile:
        profiler.disable()
        stats = lsprof.Stats(profiler.getstats())
        stats.sort()
        stats.pprint(top=10, file=sys.stderr, climit=5)

def run_worker(worker_cls, options, arguments):
    # Ignore ^C in the children.  The parent process will catch it and kill us.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    w = worker_cls(options, arguments)
    w.main()
        
def run_queenbee(queenbee_cls, options, arguments):
    debug('Launching %r; options %r; arguments %r', queenbee_cls, options, arguments)
    
    c = queenbee_cls(options, arguments)
    c.main()

def run_monitor(workers):
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGHUP, handle_signal)
    
    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SignalReceived):
        print "Terminating worker bees..."
        for worker in workers:
            worker.terminate()
        
        time.sleep(5)
        
        print "Cleaning up remaining worker bees..."
        for worker in workers:
            try:
                os.kill(worker.pid, signal.SIGKILL)
            except:
                pass
        
        print "done."
    
    
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

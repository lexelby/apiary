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
import pkgutil
import importlib

import apiary
import apiary.tools.debug
from apiary.tools.debug import *


def main(args=sys.argv[1:]):
    options, arguments = parse_args(args)
    if options.debug:
        apiary.tools.debug.enable_debug()

    if options.profile:
        from apiary.tools import lsprof
        profiler = lsprof.Profiler()
        profiler.enable(subcalls=True)

    beekeeper = apiary.BeeKeeper(options, arguments)
    beekeeper.start()

    if options.profile:
        profiler.disable()
        stats = lsprof.Stats(profiler.getstats())
        stats.sort()
        stats.pprint(top=10, file=sys.stderr, climit=5)


def get_protocol_modules():
    path = os.path.join(os.path.dirname(__file__), 'protocols')

    modules = {}

    for loader, name, is_package in pkgutil.iter_modules([path]):
        if not is_package:
            modules[name] = importlib.import_module('apiary.protocols.%s' % name)

    return modules


def parse_args(args=[]):
    parser = build_option_parser()

    modules = get_protocol_modules()
    for mod in modules.values():
        if hasattr(mod, 'add_options'):
            mod.add_options(parser)

    options, args = parser.parse_args(args)
    options.protocols = modules

    return options, args


def build_option_parser():
    parser = optparse.OptionParser()
    apiary.add_options(parser)
    return parser


if __name__ == '__main__':
    main()

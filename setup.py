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

import os
import glob
from setuptools import find_packages, setup
from apiary import __version__

setup(
    # Metadata:
    name='apiary',
    version=__version__,
    description='Distributed load testing framework',
    author='Linden Lab',
    author_email='apiary@lists.secondlife.com',
    url='http://wiki.secondlife.com/wiki/Apiary',
    long_description="""
    Apiary is a distributed, protocol-independent load testing framework that
    replays captured queries, simulating production load patterns.""",
    classifiers=[
    'Environment :: Console',
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python",
    "Operating System :: POSIX",
    "Topic :: Internet",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Intended Audience :: Developers",
    'Intended Audience :: System Administrators',
    "Development Status :: 4 - Beta"],

    # Packaging config:
    scripts=([os.path.join('bin', 'apiary')] +
             glob.glob('bin' + os.sep + '*.py') +
             glob.glob('bin' + os.sep + '*.sh')),

    packages=find_packages(exclude=['test']),

    )


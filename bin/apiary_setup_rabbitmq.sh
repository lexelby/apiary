#!/bin/sh
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


if [ $EUID != 0 ]
then
    echo "You must be root to set up rabbitmq."
    exit 1
fi

RMQCTL=rabbitmqctl

if [ -z "`which $RMQCTL`" ]
then
    echo "$RMQCTL not found in your path.  Make sure that rabbitmq is installed and that your path is correct."
    exit 1
fi

VERSION=$($RMQCTL status | grep RabbitMQ | cut -d ',' -f 4 | sed 's/["}]//g')

if [ -z "$VERSION" ]
then

    echo "Error retrieving rabbitmq version.  Is the rabbitmq server running?"
    exit 1
fi

if [ "$( echo $VERSION | sed 's/\.//g')" -lt 160 ]
then
    echo "rabbitmq version 1.6 is required.  Version $VERSION is currently installed."
    exit 1
fi

$RMQCTL delete_vhost /apiary
$RMQCTL delete_user apiary
$RMQCTL add_user apiary beehonest
$RMQCTL add_vhost /apiary
$RMQCTL set_permissions -p /apiary apiary '.*' '.*' '.*'




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

"""filtertools - Filter utilities

Functions:
filterthru - Transform a sequence through a stack of filters

"""

__all__ = [
    'filterthru',
    ]


def _append_lists(a, b):
    return a + b
    
def filterthru(data, stack):
    """Return a sequence transformed through a stack of filters
    
    The data argument is a list of zero or more items.

    The stack argument is a list of zero or more filters.  Each filter is a
    function that should take a single data item, and return a list of zero
    or more data items. Filters may simply transform the data, returning a
    single valued list, may add values by returning multiple valued list, or
    eliminate an item from further processing by return an empty list.
    
    For each filter in the stack, this function takes each item in the data
    and passes it through the filter, concatenating the results. This new,
    transformed data list becomes the input for processing with the next filter 
    in the stack. The final transformed list is the result.
    
    Note: The order of the results is maintained when concatenating.
    Note: If at any stage, there resulting list is empty, the final result
    will be empty.
    
    """
    
    for filter in stack:
        data = reduce(_append_lists, map(filter, data), [])
    return data

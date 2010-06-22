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

import unittest
from cStringIO import StringIO
from apiary.tools.codec import Message, FormatError
    

class CodecTests (unittest.TestCase):
    def testCodecSymmetry(self):
        for body, hdrs in PositiveTestVectors:
            msgin = Message(body, **hdrs)
            msgout = Message.decode_from_string(msgin.encode_to_string())
            self.failUnlessMessagesEqual(msgin, msgout)
            
    def testStreamSymmetry(self):
        msgs = []
        
        f = StringIO()
        for body, hdrs in PositiveTestVectors:
            msgin = Message(body, **hdrs)
            msgin.encode_to_file(f)
            msgs.append(msgin)

        f.seek(0)

        for msgin in msgs:
            msgout = Message.decode_from_file(f)
            self.failUnlessMessagesEqual(msgin, msgout)
            
    def testNegativeEncoding(self):
        for body, hdrs in NegativeTestVectors:
            try:
                Message(body, **hdrs).encode_to_string()
            except FormatError:
                continue # This negative test passes.
            self.fail('Constructed an invalid message: body %r; headers %r' % (body, hdrs))
                
    def failUnlessMessagesEqual(self, a, b):
        self.failUnlessEqual(a.headers, b.headers)
        self.failUnlessEqual(a.body, b.body)
            
        


PositiveTestVectors = [
    ('', {}),
    ('foo', {'fruit': 'banana'}),
    ('x' * (2**20), {'description': 'A mebibyte of xs.',
                     'reason': 'Pure OSSMness.'}),
    ]


NegativeTestVectors = [
    ('', {'body_length': 7}),
    ('', {'body_length': -1}),
    ('bad header type', {'bad_header': object}),
    ]

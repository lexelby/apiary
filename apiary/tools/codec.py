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
codec provides abstractions for messages encodings which live in message queues or data files.
'''

import re
import simplejson
from cStringIO import StringIO


class Message (object):
    '''
    A message has an arbitrary structure of headers and an opaque byte-sequence body.

    The headers structure is a dict containing any values which can be encoded with json.
    '''
    MaxElementSize = 2 ** 27 # 128 MiB
    BodyLengthKey = 'body_length'
    PrologTemplate = '# Apiary Message: %d header bytes.\n'
    PrologPattern = re.compile(r'# Apiary Message: (\d+) header bytes.')
    
    @classmethod
    def decode_many_from_file(cls, fp):
        '''
        This returns an iterator which yields messages as they are parsed.
        '''
        m = cls.decode_from_file(fp)
        while m:
            yield m
            m = cls.decode_from_file(fp)

    @classmethod
    def decode_from_string(cls, source):
        return cls.decode_from_file(StringIO(source))
    
    @classmethod
    def decode_from_file(cls, fp):
        '''
        Given a file-like object, return a Message instance, None, or raise a FormatException.

        None is raised if the file is at EOF.
        '''
        prolog = fp.readline()
        if prolog == '':
            return None
        m = cls.PrologPattern.match(prolog)
        if m is None:
            raise FormatError('Could not decode prolog: %r', prolog)
        hlen = int(m.group(1))
        if hlen > cls.MaxElementSize:
            raise FormatError('Prolog header length %d is larger than the maximum allowed %d bytes.',
                              hlen,
                              cls.MaxElementSize)
        headerchunk = fp.read(hlen)
        newline = fp.read(1)
        if newline != '\n':
            raise FormatError('Headers not terminated by a newline.')
        headers = simplejson.loads(headerchunk)
        if type(headers) is not dict:
            raise FormatError('Headers must be a mapping, but stream contains: %r',
                              headers)
        # Translate the keys to utf8 so that ** application works:
        headers = dict( ((k.encode('utf8'), v) for (k, v) in headers.items()) )
        
        bodylength = headers.get(cls.BodyLengthKey)
        if type(bodylength) is not int or not (0 <= bodylength < cls.MaxElementSize):
            raise FormatError('Invalid %r header: %r',
                              cls.BodyLengthKey,
                              bodylength)
        body = fp.read(bodylength)
        newline = fp.read(1)
        if newline != '\n':
            raise FormatError('Body not terminated by a newline.')
        try:
            return cls(body, **headers)
        except TypeError, e:
            raise FormatError('Invalid headers - %s: %r',
                              ' '.join(e.args),
                              headers)
            
    def __init__(self, body, **headers):
        if headers.has_key(self.BodyLengthKey):
            lenhdr = headers.pop(self.BodyLengthKey)
            if len(body) != lenhdr:
                raise FormatError('Incorrect %s header: %d != %d', self.BodyLengthKey, lenhdr, len(body))
        self.body = body
        self.headers = headers

    def encode_to_string(self):
        f = StringIO()
        self.encode_to_file(f)
        return f.getvalue()
    
    def encode_to_file(self, fp):
        hdrs = self.headers.copy()
        hdrs[self.BodyLengthKey] = len(self.body)

        try:
            headerchunk = simplejson.dumps(hdrs)
        except TypeError, e:
            raise FormatError('Failure to encode headers - %s: %r',
                              ' '.join(e.args),
                              hdrs)
        hlen = len(headerchunk)
        if hlen > self.MaxElementSize:
            raise FormatError('Header encoding is %d bytes which is larger than the maximum allowed %d bytes.',
                              hlen,
                              self.MaxElementSize)
        
        fp.write(self.PrologTemplate % hlen)
        fp.write(headerchunk + '\n')
        fp.write(self.body + '\n')


class FormatError (Exception):
    def __init__(self, tmpl, *args):
        Exception.__init__(self, tmpl % args)

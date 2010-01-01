"""unittests for naglfar

Copyright (c) 2009, Erik Gorset
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
  * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
  * Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY ERIK GORSET, AND CONTRIBUTORS ``AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import sys
import socket
import unittest
from naglfar import *

class Tests(unittest.TestCase):
    def _pair(self):
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        c, d = ScheduledFile.fromSocket(a), ScheduledFile.fromSocket(b)
        c.autoflush = d.autoflush = True
        return c, d

    def testSimple(self):
        c, d = self._pair()

        hello = 'hello world\n'
        c.write(hello)
        self.assertEquals(d.readline(), hello)
        d.write('a')
        d.write('b')
        d.write('c')
        d.write('\n')
        self.assertEquals(c.readline(), 'abc\n')

        c.write(hello)
        self.assertEquals(d.read(len(hello)), hello)
        c.write(hello)
        self.assertEquals(''.join(d.read(1) for i in xrange(len(hello))), hello)

        c.close()
        d.close()

    def testPartial(self):
        a, b = self._pair()
        a.write('foo')
        a.flush()
        a.close()
        self.assertEquals(b.readline(), 'foo')

    def testConcurrent(self):
        c, d = self._pair()

        @go
        def runner():
            blob = c.readline()
            self.assertEquals(blob, 'foobar\n')
            c.write('hello: ' + blob)

        d.write('foobar\n')
        self.assertEquals(d.readline(), 'hello: foobar\n')
        c.close()
        d.close()

    def testBig(self):
        c, d = self._pair()

        blob = '\n'*(5*1024**2)
        go(c.write, blob)
        self.assertEquals(d.read(len(blob)), blob)

        c.close()
        d.close()

    def testFew(self):
        for i in xrange(10):
            testScheduledServer(i)

    def testUntil(self):
        c, d = self._pair()
        c.write('aafoobar')
        self.assertEquals('aafoobar', ''.join(d.readUntil('foobar')))
        c.write('aaa')
        self.assertEquals('a', ''.join(d.readUntil('a')))
        self.assertEquals('a', ''.join(d.readUntil('a')))
        self.assertEquals('', ''.join(d.readUntil('')))
        self.assertEquals('a', ''.join(d.readUntil('a')))

        c.write('a')
        self.assertEquals('', ''.join(d.readUntil('a', False)))
        self.assertEquals('', ''.join(d.readUntil('a', False)))
        self.assertEquals('', ''.join(d.readUntil('a', False)))
        self.assertEquals('a', d.read(1))

        c.write('abc')
        self.assertEquals('a', ''.join(d.readUntil('b', False)))
        self.assertEquals('b', ''.join(d.readUntil('c', False)))

        c.close()
        self.assertEquals('c', d.read())

if __name__ == "__main__":
    unittest.main()

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
import os
import sys
import errno
import socket
import unittest
import collections

from naglfar import *
from naglfar.sendfile import sendfile
from naglfar import objects

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

    def _pair2(self):
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        return a, b

    def testSendfile1(self):
        fd = open(__file__)
        data = fd.read()
        a, b = self._pair2()
        n = sendfile(fd.fileno(), a.fileno(), 0, 42)
        self.assertEquals(n, 42)
        self.assertEquals(data[:42], b.recv(1024))

        n = sendfile(fd.fileno(), b.fileno(), len(data) - 10, 42)
        self.assertEquals(n, 10)
        self.assertEquals(data[len(data) - 10:], a.recv(1024))

        n = sendfile(fd.fileno(), b.fileno(), len(data), 42)
        self.assertEquals(n, 0)
        fd.close()

    if sys.platform != 'linux2':
        def testSendfileZero(self):
            a, b = self._pair2()
            fd = open(__file__)
            data = fd.read()
            n = sendfile(fd.fileno(), b.fileno(), 0, 0)
            self.assertTrue(n > 0)
            self.assertEquals(data[:n], a.recv(n + 1024))

        def testSendfileHeaders(self):
            fd = open(__file__)
            data = fd.read(10)
            dataM = 'XX' + data + 'YY'
            a, b = self._pair2()
            n = sendfile(fd.fileno(), a.fileno(), 0, 10, ['XX'], ['YY'])

            output = b.recv(1024)
            self.assertEquals(n, len(output))
            self.assertEquals(n, len(dataM))
            self.assertEquals(dataM, output)

    def testSendfile3(self):
        fd = open(__file__)

        a, b = self._pair2()
        n = sendfile(fd.fileno(), a.fileno(), 0, 1)
        b.recv(1)
        b.close()
        try:
            n = sendfile(fd.fileno(), a.fileno(), 0, 1)
        except OSError, e:
            self.assertTrue(e.errno in (errno.ENOTCONN, errno.EPIPE)) # linux will throw EPIPE
        else:
            self.assertTrue(False, 'exception not raised')

    def testSendfile4(self):
        fd = open(__file__)
        a, b = self._pair2()
        total = os.fstat(fd.fileno()).st_size
        while True:
            try:
                n = sendfile(fd.fileno(), a.fileno(), 0, total)
                self.assertTrue(n > 0)
            except OSError, e:
                self.assertEquals(e.errno, errno.EAGAIN)
                break
        fd.close()
        a.close()
        b.close()

    def testSendfile5(self):
        a, b = self._pair()
        fd = open(__file__)
        data = open(__file__).read()

        @go
        def r():
            n = a.sendfile(fd.fileno(), nbytes=len(data))
            self.assertEquals(n, len(data))
            a.close()

        all = b.read()
        self.assertEquals(len(all), len(data))
        self.assertEquals(all, data)
        b.close()

        fd = open(__file__)
        c, d = self._pair()
        @go
        def r():
            n = c.sendfile(fd.fileno(), 10, len(data)-10)
            self.assertEquals(n, len(data) - 10)
            c.close()
        self.assertEquals(d.read(), data[10:])
        d.close()

    def testNamedTuple(self):
        obj = 1,2
        out, = objects.loadstream(objects.dumpstream([obj]))
        self.assertEquals(obj, out)

        obj = collections.namedtuple('a', 'b c')(1,2)
        out, = objects.loadstream(objects.dumpstream([obj]))
        self.assertEquals(obj, out)

    def testUnicode(self):
        blob = objects.dumps(u'hei')
        s = objects.loads(blob)
        self.assertEquals(type(s), unicode)
        self.assertEquals(s, u'hei')

        s2 = objects.loads(objects.dumps(('hei', u'hei')))
        self.assertEquals(tuple(type(i) for i in s2), (str, unicode))
        self.assertEquals(s2, ('hei', u'hei'))

    def _pair3(self):
        a, b = self._pair()
        a.__class__ = b.__class__ = ObjectFile
        return a, b

    def testObjectFile(self):
        a, b = self._pair3()
        
        obj1 = (42, 'asdf', ['hehe'])
        obj2 = 'hei verden'

        @go
        def client():
            a.writeObject(obj1)
            a.writeObject(obj2)
            a.write('must work')
            a.close()

        self.assertEquals(b.readObject(), obj1)
        self.assertEquals(b.readObject(), obj2)
        self.assertEquals(b.read(), 'must work')

    def testBatch(self):
        a = Channel()
        self.assertEquals(a.readWaiting(), [])
        a.write(1)
        self.assertEquals(a.readWaiting(), [1])
        a.write(2)
        a.write(3)
        self.assertEquals(a.readWaiting(), [2, 3])

        @go
        def w():
            a.write(4)
            a.write(5)

        self.assertEquals(a.readWaiting(block=True), [4, 5])

        foo = iter(a)

        @go
        def w():
            a.write(6)
            a.write(7)

        self.assertEquals(foo.next(), 6)
        self.assertEquals(foo.next(), 7)

    def testObjectStream(self):
        a, b = self._pair3()
        a.writeObject(1)
        a.writeObject(2)
        a.write('garbage')
        a.close()
        self.assertEquals(list(b.readObjectStream()), [1, 2])

    def testSize(self):
        self.assertEquals(objects.unpackHeader1(0), (32, 32, 0))
        self.assertEquals(objects.unpackHeader1(int('11111111', 2)), (28, 28, 3))
        self.assertEquals(objects.unpackHeader1(int('01011111', 2)), (10, 30, 3))

        self.assertEquals(objects.unpackHeader2('aaa', 8, 16), (97, 24929))
        self.assertEquals(objects.unpackHeader2('\xff', 4, 4), (15, 15))

    def testHeader(self):
        def t(id, type, length):
            data = objects.marshalHeader(id, type, length)
            id_size, length_size, header_type = objects.unpackHeader1(ord(data[0]))
            self.assertEquals(header_type, type)
            self.assertEquals(objects.parseHeader(objects.marshalHeader(id, type, length)), (id, type, length))

        t(0, 0, 0)
        t(1, 2, 1)
        t(1, 3, 1)
        t(0, 1, 4)
        for x in xrange(32):
            for y in xrange(32):
                t(x, 1, y)

    def testObjects(self):
        obj = 0, objects.TYPE_INTEGER, 42
        data = ''.join(objects.marshal([obj]))
        self.assertEquals(list(objects.unmarshal([data])), [obj])

if __name__ == "__main__":
    unittest.main()

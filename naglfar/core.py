"""Asynchronous IO library for python using greenlet based coroutines.

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


Here's an implementation of a scheduler/Channel library which can be used to
implement servers using coroutines and asynchronous IO. It provides a
SocketServer mix-in which can be combined with BaseHTTPServer to implement a
comet enabled server which can support a high number of concurrent connections.

To demonstrate the capabilities of the library, an example of a handler for
BaseHTTPServer is shown to tackle the c10k problem, and to be an excellent
comet enabled server.

Please note that there's nothing strange with the handler implementation. By
providing a thread or fork compatible implementation of Channel, it should be
possible to run it with the builtin forking or threading SocketServer mixins.
"""
import BaseHTTPServer

class TestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    waiters = [] # global list we use to track all clients waiting to be notified
    def do_GET(self):
        # send headers
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

        if self.path == '/wait': # wait for result
            c = Channel()
            self.waiters.append(c)
            n = c.read()

        elif self.path == '/notify': # notify everyone
            n = len(self.waiters)
            for i in self.waiters:
                i.write(n)
            del self.waiters[:]

        else: # current number of waiters
            n = len(self.waiters)

        self.wfile.write('%s' % n)

    def log_message(self, *args, **vargs):
        pass # mute log messages

"""
Then we need a mix-in to handle scheduler/channel activities. This is where we
put the magic, and is comperable to ThreadingMixIn and ForkingMixIn.
"""

class ScheduledMixIn:
    "Mix-in class to handle each request in a new coroutine"

    def process_request(self, request, client_address):
        # the BaseHTTPServer framework uses only the "file protocol" for a file
        # descriptors, so we put the request in an object which will wrap all
        # IO calls using kqueue/epoll and schedule/Channel.
        request = ScheduledFile.fromSocket(request)
        @go
        def runner():
            self.finish_request(request, client_address)
            self.close_request(request)

    def handle_request(self):
        return self._handle_request_noblock()

    def serve_forever(self):
        while True:
            self.handle_request()

    def server_activate(self):
        self.socket.listen(self.request_queue_size)
        self.socket.setblocking(False)
        self.acceptStream = Channel() # use a channel for new connections
        def runner(n, eof):
            for i in xrange(n): # kqueue will provide the number of connections waiting
                try:
                    client = self.socket.accept()
                except socket.error, e:
                    if e.errno == errno.EAGAIN: # either epoll, a kernel bug or a race condition
                        break
                    # FIXME: more error handling?
                    raise
                self.acceptStream.write(client)
            if not eof:
                return runner
        _goRead(self.socket.fileno(), runner)

    def get_request(self):
        return self.acceptStream.read()

"""
To test this we will first start the server, create N clients that will
connect and wait, then finally connect with a client that notifies everyone. At
the same time we will continuously connect a client to get the status. 
"""

def testScheduledServer(n):
    "test http server with n clients"
    class ScheduledHTTPServer(ScheduledMixIn, BaseHTTPServer.HTTPServer):
        pass
    # start web server at a random port
    httpd = ScheduledHTTPServer(('', 0), TestHandler)
    address = httpd.server_name, httpd.server_port
    go(httpd.serve_forever)

    def httpClientGet(client, path):
        "super simple http client"
        try:
            client.write('GET %s HTTP/1.0\r\n\r\n' % path)

            data = ''.join(client)
            pos = data.find('\r\n\r\n')
            return data[:pos], data[pos+4:] 
        finally:
            client.close()

    # spin up a few clients
    for i in xrange(n):
        def runner(client):
            header, body = httpClientGet(client, '/wait')
            assert int(body) == n
        go(partial(runner, ScheduledFile.connectTcp(address)))

    # wait until all clients are ready
    count = 0
    while count != n:
        header, body = httpClientGet(ScheduledFile.connectTcp(address), '/')
        count = int(body)

    # notify everyone
    header, body = httpClientGet(ScheduledFile.connectTcp(address), '/notify')
    assert int(body) == n

    # wait for everyone to finish
    count = -1
    while count:
        header, body = httpClientGet(ScheduledFile.connectTcp(address), '/')
        count = int(body)

"""
Example run of testScheduledServer on a mbp 13" 2.53 GHz:

    % python naglfar/core.py 10000
    done 10000

    Time spent in user mode   (CPU seconds) : 10.567s
    Time spent in kernel mode (CPU seconds) : 3.344s
    Total time                              : 0:15.67s
    CPU utilisation (percentage)            : 88.7%

Even though it's only 10k clients, they are all running in the same
process/thread as the server, which makes it 20k sockets. The amount of time
spent in user mode vs kernel mode tells us that python makes up for about 75%
of the cpu usage.

The next step would perhaps be to look into optimizing BaseHTTPServer. A faster
language or implementation should make it possible to get closer to the 3.344s
theretical limit using the same design. It's also likely that the kernel itself
could be optimized for this kind of work. The only tuning performed was
increasing the maximum number of descriptors.

Finally, the code to make it all work:
"""

import os
import errno
import select
import socket
import traceback

from greenlet import greenlet, getcurrent
from functools import partial
from collections import deque, namedtuple

# This is just a job queue which we routinely pop to do more work. There's no
# "switch thread after N time" mecanism, so each job needs to behave. 
queue = deque()
def go(callable, *args, **vargs):
    "Create a new coroutine for callable(*args, **vargs)"
    def runner():
        callable(*args, **vargs)
        scheduler.switch() # switch back the scheduler when done
    g = greenlet(runner, scheduler) # scheduler must be parent
    queue.append(g.switch)

def scheduler():
    try:
        while queue:
            queue.popleft()()
    except Exception, e:
        traceback.print_exc()
        os._exit(1)
scheduler = greenlet(scheduler)

class Channel(object):
    "An asynchronous channel"
    def __init__(self):
        self.q = deque()
        self.waiting = []

    def write(self, msg):
        "Write to the channel"
        self.q.append(msg)
        # notify everyone
        queue.extend(self.waiting)
        self.waiting = []

    def wait(self):
        while not self.q:
            # block until we have data
            self.waiting.append(getcurrent().switch)
            scheduler.switch()

    def read(self):
        "Read from the channel, blocking if it's empty"
        self.wait()
        return self.q.popleft()

    def readWaiting(self, block=False):
        if block:
            self.wait()
        result = list(self.q)
        self.q.clear()
        return result

    def iterateWaiting(self):
        while True:
            yield self.readWaiting(True)

    def __iter__(self):
        while True:
            yield self.read()

def goRead(fd, n=None):
    "Read n bytes, or the next chunk if n is None"
    c = Channel()
    buffer = bytearray()

    def reader(bytesReady, eof):
        if bytesReady:
            # read maxmium or the bytes remaing
            try:
                data = os.read(fd, bytesReady if n is None else min(bytesReady, n - len(buffer)))
            except OSError, e:
                if e.errno == errno.EAGAIN: # potentation race condition
                    return reader
                data = ''
            eof = not data
            buffer.extend(data)
            if not eof and n is not None and len(buffer) < n:
                return reader
        c.write(str(buffer))
    _goRead(fd, reader)
    return c.read

def goWrite(fd, data):
    "Write data to fd and return the number of bytes written"
    o = dict(offset=0)
    c = Channel()

    def writer(bytesReady, eof):
        offset = o['offset']
        if not eof:
            try:
                offset += os.write(fd, str(data[offset:offset+bytesReady]))
            except OSError, e:
                if e.errno == errno.EAGAIN:
                    pass
                eof = True # treat all other errors as eof
            if not eof and offset < len(data):
                o['offset'] = offset
                return writer
        c.write(offset)
    _goWrite(fd, writer)
    return c.read

def goSendfile(fdFile, fd, offset, nbytes):
    assert type(fd) == int
    assert nbytes > 0
    o = dict(offset=offset, nbytes=nbytes)
    c = Channel()

    def writer(bytesReady, eof):
        if bytesReady and not eof:
            try:
                n = sendfile(fdFile, fd, o['offset'], min(bytesReady, o['nbytes']))
            except OSError, e:
                if e.errno == errno.EAGAIN:
                    return writer
                return # do more here?
            o['offset'] += n
            o['nbytes'] -= n
            assert o['nbytes'] >= 0
            if n and o['nbytes']:
                return writer
        c.write(o['offset'] - offset)

    _goWrite(fd, writer)
    return c.read

def goClose(fd):
    "Close the fd and do kqueue cleanup"
    assert fd != -1 and fd is not None
    # file descriptors are reused instantly, so we need to remove any left overs
    _goClose(fd)
    os.close(fd)

if hasattr(select, 'epoll'):
    epoll = select.epoll()
    io = {}
    ioState = {}

    def _ioCore():
        for fd, eventmask in epoll.poll(0 if queue else -1):
            assert not eventmask & select.EPOLLPRI
            removeMask = 0
            for mask in (select.EPOLLIN, select.EPOLLOUT):
                key = fd, mask
                if eventmask & mask:
                    callback = io.pop(key)(32768, bool(eventmask & (select.EPOLLHUP | select.EPOLLERR)))
                    if callback:
                        assert key not in io
                        io[key] = callback
                    else:
                        removeMask |= mask
            if removeMask:
                ioState[fd] ^= removeMask
                epoll.modify(fd, ioState[fd])
        return bool(io)

    def _goEpoll(ident, mask, m):
        if ident not in ioState:
            ioState[ident] = mask
            epoll.register(ident, mask)
        else:
            ioState[ident] = eventmask = ioState[ident] | mask
            epoll.modify(ident, eventmask)
        io[ident, mask] = m
        _ioRunner.activate()

    _goWrite = lambda fd, m:_goEpoll(fd, select.EPOLLOUT, m)
    _goRead  = lambda fd, m:_goEpoll(fd, select.EPOLLIN,  m)

    def _goClose(fd):
        if fd in ioState:
            del ioState[fd]
            for key in (fd, select.EPOLLIN), (fd, select.EPOLLOUT):
                if key in io:
                    del io[key]

elif hasattr(select, 'kqueue'):
    import patch_kqueue # kqueue is broken in python <=2.6.4. This will fix it using ctypes

    kq = select.kqueue()
    io = {}
    ioChanges = {}

    def _ioCore():
        "Add changes and poll for events, blocking if scheduler queue is empty"
        changes = ioChanges.values()
        ioChanges.clear()
        for event in kq.control(changes, len(io), 0 if queue else None):
            assert not event.flags & select.KQ_EV_ERROR
            key = event.ident, event.filter
            callback = io.pop(key)(event.data, bool(event.flags & select.KQ_EV_EOF))
            if callback:
                assert key not in io
                io[key] = callback
            else:
                ioChanges[key] = select.kevent(event.ident, event.filter, select.KQ_EV_DELETE)
        return bool(io)
    def _goRead(fd, m):
        ioChanges[fd, select.KQ_FILTER_READ] = select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_ENABLE)
        io[fd, select.KQ_FILTER_READ] = m
        _ioRunner.activate()
    def _goWrite(fd, m):
        ioChanges[fd, select.KQ_FILTER_WRITE] = select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_ADD | select.KQ_EV_ENABLE)
        io[fd, select.KQ_FILTER_WRITE] = m
        _ioRunner.activate()
    def _goClose(fd):
        for key in (fd, select.KQ_FILTER_WRITE), (fd, select.KQ_FILTER_READ):
            if key in io:
                del io[key]
            if key in ioChanges:
                del ioChanges[key]

else: # fallback to select
    ioRead = {}
    ioWrite = {}
    
    def _ioCore():
        x, y, z = select.select(list(ioRead), list(ioWrite), [], 0 if queue else None)
        for fds, l in ((x, ioRead), (y, ioWrite)):
            for fd in fds:
                callback = l.pop(fd)(32768, False)
                if callback:
                    assert fd not in l
                    l[fd] = callback
        return bool(ioRead or ioWrite)
    def _goRead(fd, m):
        ioRead[fd] = m
        _ioRunner.activate()
    def _goWrite(fd, m):
        ioWrite[fd] = m
        _ioRunner.activate()
    def _goClose(fd):
        if fd in ioWrite:
            del ioWrite[fd]
        if fd in ioRead:
            del ioRead[fd]

from sendfile import sendfile

def _ioRunner():
    try:
        hasMore = _ioCore()
    except:
        traceback.print_exc()
        os._exit(2)

    if hasMore:
        queue.append(_ioRunner)
    else:
        _ioRunner.active = False
_ioRunner.active = False
def _ioActivate():
    if not _ioRunner.active:
        _ioRunner.active = True
        queue.append(_ioRunner)
_ioRunner.activate = _ioActivate


"""
BaseHTTPServer/SocketServer expect to work with file objects. All IO operations
needs to be wrapped by using the scheduler enabled goRead, goWrite and goClose.
This will ensure that other coroutines can do work while the file object is
waiting for IO.
"""

class ScheduledFile(object):
    "A file object using the scheduler/Channel framework to do asynchronous nonblocking IO"
    def __init__(self, fd, autoflush=False, bufferSize=2**16):
        self.fd = fd
        self.autoflush = autoflush
        self.bufferSize = bufferSize

        self.incoming = bytearray()
        self.outgoing = bytearray()
        self._flushers = None

        self.nwrite = self.nread = 0

    @classmethod
    def fromSocket(cls, sock, *args, **vargs):
        "Use a existing socket to make instance"
        # python closes the socket under deallocation, so we use dup to make sure
        # we can close the fd independently.
        return cls(os.dup(sock.fileno()), *args, **vargs)

    @classmethod
    def connectTcp(cls, address):
        sock = socket.socket()
        try:
            sock.setblocking(False)
            try:
                sock.connect(address)
            except socket.error, e:
                if e.errno != errno.EINPROGRESS: # means we need to wait for the socket to become writable
                    raise
            s = cls.fromSocket(sock, autoflush=True)
            goWrite(s.fd, '')() # we need to make sure it's writable before doing anything
            return s
        finally:
            sock.close()

    @property
    def closed(self):
        return self.fd is None

    def _flusher(self):
        while self.outgoing and self.fd is not None:
            n = goWrite(self.fd, self.outgoing)()
            if n == 0:
                self.outgoing = None
            else:
                del self.outgoing[:n]
                self.nwrite += n
        for i in self._flushers:
            i.write(True)
        self._flushers = None

    def flush(self, block=True):
        if self._flushers is None:
            self._flushers = []
            go(self._flusher)
        if block:
            c = Channel()
            self._flushers.append(c)
            c.read()

    def write(self, data):
        if None in (self.fd, self.outgoing):
            raise ValueError('closed')
        self.outgoing.extend(data)
        if self.autoflush:
            self.flush(block=len(self.outgoing) > self.bufferSize)
        elif len(self.outgoing) > self.bufferSize:
            self.flush()

    def _read(self, n=None):
        assert self.fd is not None
        chunk = goRead(self.fd, n)()
        self.nread += len(chunk)
        return chunk

    def readline(self, n=Ellipsis, separator='\n'):
        "Read a whole line, until eof or maximum n bytes"

        line = bytearray()
        for chunk in self.readUntil(separator):
            assert chunk
            line += chunk
            if len(line) >= n:
                break

        if len(line) == n:
            return str(line)
        elif len(line) > n:
            assert not self.incoming
            self.incoming = line[n:]
            return str(line[:n])
        else:
            return str(line)

    def read(self, n=-1):
        "read n bytes or until eof"
        if n == -1:
            while True:
                chunk = self._read()
                if not chunk:
                    break
                self.incoming += chunk
        else:
            while n > len(self.incoming):
                chunk = self._read()
                self.incoming += chunk
                if not chunk:
                    break
        data = str(self.incoming[:n if n != -1 else len(self.incoming)])
        del self.incoming[:len(data)]
        return data

    def readUntil(self, separator, includingTxt=True):
        "read until separator or eof"
        def reader():
            if self.incoming:
                chunk = str(self.incoming)
                del self.incoming[:]
                yield chunk

            while True:
                chunk = self._read()
                yield chunk
                if not chunk:
                    break

        for chunk in readUntil(reader().next, self.incoming.extend, separator):
            yield chunk

        if includingTxt:
            if self.incoming:
                assert self.incoming.startswith(separator)
                del self.incoming[:len(separator)]
                yield separator

    def close(self, flush=True):
        if self.fd is None:
            return
        if flush and self.outgoing:
            self.flush()
        goClose(self.fd)
        self.fd = None

    def __iter__(self):
        while True:
            line = self.readline()
            if not line:
                break
            yield line

    def makefile(self, *args, **vargs):
        return self

    def sendfile(self, fd, offset=0, nbytes=0):
        self.flush()
        return goSendfile(fd, self.fd, offset, nbytes)()



def readUntil(next, pushback, separator):
    result = bytearray()

    while True:
        pos = result.find(separator)
        if pos != -1:
            rest = result[pos:]
            if rest:
                pushback(str(rest))
                del result[pos:]
            if result:
                yield str(result)
            return
        elif len(separator) < len(result):
            yield str(result[:-len(separator)])
            del result[:-len(separator)]

        chunk = next()
        if not chunk:
            if result:
                yield str(result)
                return
        result += chunk

if __name__ == "__main__":
    import sys
    n = int(sys.argv[1])
    testScheduledServer(n)
    print 'done', n

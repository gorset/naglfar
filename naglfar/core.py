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


Here's a implementation of a scheduler/Channel library which can be used to
implement servers using coroutines and asynchronous IO. It provides a
SocketServer mix-in which can be combined with BaseHTTPServer to implement a
comet enabled server which can support a high number of concurrent connection.

To demonstrate the capabilities of the library, a example of a handler for
BaseHTTPServer is shown to tackle the c10k problem in the comet spirit.

Please note that there's nothing strange with the handler implementation. By
providing a thread or fork compatible implementation of Channel, it should be
possible to run it with the builtin forking or threading SocetServer mixins.
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
        # IO calls using kqueue and schedule/Channel.
        request = DummySocket(ScheduledFile.fromSocket(request))
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
                    if e.errno == errno.EAGAIN: # either a kernel bug or a race condition
                        break
                self.acceptStream.write(client)
            if not eof:
                return runner
        _goRead(self.socket.fileno(), runner)

    def get_request(self):
        return self.acceptStream.read()

"""
To test this we will first start the server, create N clients that will
connect and wait, then finally connect with a client that notify everyone. At
the same time we will continuously connect a client to get the status. 
"""

def testScheduledServer(n):
    "test http server with n clients"
    # start web server at a random port
    class ScheduledHTTPServer(ScheduledMixIn, BaseHTTPServer.HTTPServer):
        pass
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
    g = greenlet(partial(callable, *args, **vargs), scheduler) # scheduler must be parent
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

    def read(self):
        "Read from the channel, blocking if it's empty"
        while not self.q:
            # block until we have data
            self.waiting.append(getcurrent().switch)
            scheduler.switch()
        return self.q.popleft()

"""
KQueue is a great event notification system that can be used to do asynchronous
IO. Three methods are implemented on top of kqueue, for reading, writing and
closing the sockets. KQueue is only supported on *BSD and darwin/OSX, but these
methods should be easily implemented using another event notification
systems like select or epoll.
"""

def goRead(fd, n=None):
    "Read n bytes, or the next chunk if n is None"
    c = Channel()
    buffer = bytearray()

    def reader(bytesReady, eof):
        if bytesReady:
            # read maxmium or the bytes remaing
            data = os.read(fd, bytesReady if n is None else min(bytesReady, n - len(buffer)))
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
            offset += os.write(fd, str(data[offset:offset+bytesReady]))
            if offset < len(data):
                o['offset'] = offset
                return writer
        c.write(offset)
    _goWrite(fd, writer)
    return c.read

def goClose(fd):
    "Close the fd and do kqueue cleanup"
    assert fd != -1 and fd is not None
    # file descriptors are reused instantly, so we need to remove any left overs
    _goClose(fd)
    os.close(fd)

io = {} # callbacks added here must never switch greenlet
ioChanges = {}

if hasattr(select, 'epoll'):
    epoll = select.epoll()

    def _epollRunner():
        timeout = 0 if queue else -1
        try:
            for fd, eventmask in epoll.poll(timeout):
                assert not eventmask & select.EPOLLERR
                assert not eventmask & select.EPOLLPRI
                removeMask = 0
                for mask in (select.EPOLLIN, select.EPOLLOUT):
                    key = fd, mask
                    if eventmask & mask:
                        callback = io.pop(key)(32768, eventmask & select.EPOLLHUP)
                        if callback:
                            assert key not in io
                            io[key] = callback
                        else:
                            removeMask |= mask
                if removeMask:
                    ioChanges[fd] ^= removeMask
                    epoll.modify(fd, ioChanges[fd])
        except:
            traceback.print_exc()
            os._exit(2)

        # add it back to the queue if we have more IO to do
        if io:
            queue.append(_epollRunner)
        else:
            _epollRunner.active = False
    _epollRunner.active = False

    def _goEpoll(ident, mask, m):
        if ident not in ioChanges:
            ioChanges[ident] = mask
            epoll.register(ident, mask)
        else:
            ioChanges[ident] = eventmask = ioChanges[ident] | mask
            epoll.modify(ident, eventmask)
        io[ident, mask] = m

        if not _epollRunner.active:
            _epollRunner.active = True
            queue.append(_epollRunner)

    _goWrite = lambda fd, m:_goEpoll(fd, select.EPOLLOUT, m)
    _goRead  = lambda fd, m:_goEpoll(fd, select.EPOLLIN,  m)

    def _goClose(fd):
        if fd in ioChanges:
            epoll.unregister(fd)
            del ioChanges[fd]
            for key in (fd, select.EPOLLIN), (fd, select.EPOLLOUT):
                if key in ioChanges:
                    del ioChanges[key]

elif hasattr(select, 'kqueue'):
    import patch_kqueue # kqueue is broken in python <=2.6. This will fix it using ctypes

    kq = select.kqueue()

    def _kqueueRunner():
        "Add changes and poll for events, blocking if scheduler queue is empty"
        timeout = 0 if queue else None 
        changes = ioChanges.values()
        ioChanges.clear()
        try:
            for event in kq.control(changes, len(io), timeout):
                assert not event.flags & select.KQ_EV_ERROR
                key = event.ident, event.filter
                callback = io.pop(key)(event.data, bool(event.flags & select.KQ_EV_EOF))
                if callback:
                    assert key not in io
                    io[key] = callback
                else:
                    ioChanges[key] = select.kevent(event.ident, event.filter, select.KQ_EV_DELETE)
        except:
            traceback.print_exc()
            os._exit(2)

        # add it back to the queue if we have more IO to do
        if io:
            queue.append(_kqueueRunner)
        else:
            _kqueueRunner.active = False
    _kqueueRunner.active = False

    def _goKqueue(ident, filter, m):
        "Add a filter for a fd with a callback"
        assert type(ident) == int and ident != -1
        key = ident, filter
        assert key not in io

        ioChanges[key] = select.kevent(ident, filter, select.KQ_EV_ADD | select.KQ_EV_ENABLE)
        io[key] = m

        # add the kqueue runner to the scheduler queue if it's not already there
        if not _kqueueRunner.active:
            _kqueueRunner.active = True
            queue.append(_kqueueRunner)

    _goWrite = lambda fd, m:_goKqueue(fd, select.KQ_FILTER_WRITE, m)
    _goRead  = lambda fd, m:_goKqueue(fd, select.KQ_FILTER_READ,  m)

    def _goClose(fd):
        for key in (fd, select.KQ_FILTER_WRITE), (fd, select.KQ_FILTER_READ):
            if key in io:
                io.pop(key)
            if key in ioChanges:
                del ioChanges[key]

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
            del self.outgoing[:n]
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
        self.outgoing.extend(data)
        if self.autoflush:
            self.flush(block=len(self.outgoing) > self.bufferSize)
        elif len(self.outgoing) > self.bufferSize:
            self.flush()

    def readline(self, n=None):
        "Read a whole line, until eof or maximum n bytes"
        if '\n' not in self.incoming:
            while n is None or len(self.incoming) < n:
                chunk = goRead(self.fd)()
                self.incoming += chunk
                if not chunk or '\n' in chunk:
                    break

        pos = self.incoming.find('\n') + 1 or len(self.incoming)
        line = str(self.incoming[:pos])
        del self.incoming[:pos]
        return line

    def read(self, n=-1):
        "read n bytes or until eof"
        self.incoming+= goRead(self.fd, n - len(self.incoming) if n != -1 else None)()
        data = str(self.incoming[:n])
        del self.incoming[:n]
        return data

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

# A dummy socket, with the only objective of returing the ScheduledFile object
# when made into a file.
DummySocket = namedtuple('DummySocket', 'file')
DummySocket.makefile = lambda self,*x,**y:self.file
DummySocket.close = lambda self:self.file.close()

if __name__ == "__main__":
    import sys
    n = int(sys.argv[1])
    testScheduledServer(n)
    print 'done', n

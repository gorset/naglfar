"""Patch broken kqueue

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
import errno
import ctypes
import ctypes.util
import select

if sys.platform == 'linux2':
    raise ImportError('kqueue not supported on linux')

class Event(ctypes.Structure):
    if sys.platform.startswith('netbsd'):
        _fields_ = [
            ('ident', ctypes.c_uint64),
            ('filter', ctypes.c_uint32),
            ('flags', ctypes.c_uint32),
            ('fflags', ctypes.c_uint32),
            ('data', ctypes.c_int64),
            ('udata', ctypes.c_int64),
        ]
    elif ctypes.sizeof(ctypes.c_void_p) == 8:
        _fields_ = [
            ('ident', ctypes.c_uint64),
            ('filter', ctypes.c_short),
            ('flags', ctypes.c_ushort),
            ('fflags', ctypes.c_uint),
            ('data', ctypes.c_int64),
            ('udata', ctypes.c_int64),
        ]
    else:
        _fields_ = [
            ('ident', ctypes.c_uint),
            ('filter', ctypes.c_short),
            ('flags', ctypes.c_ushort),
            ('fflags', ctypes.c_uint),
            ('data', ctypes.c_int),
            ('udata', ctypes.c_int),
        ]

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

try:
    numbers = float, int, long
except NameError:
    numbers = float, int

class _timespec(ctypes.Structure):
    _fields_ = [
        ('tv_sec', ctypes.c_long),
        ('tv_nsec', ctypes.c_long),
    ]

    @classmethod
    def convert(self, time):
        sec = int(time)
        nsec = int(time*10**9) - sec * 10**9

        return _timespec(sec, nsec)

def kevent(kq, changelist=None, nevents=0, timeout=None):
    if type(kq) != int:
        kq = kq.fileno()
    if timeout is not None:
        if type(timeout) in numbers:
            timeout = ctypes.pointer(_timespec.convert(timeout))
        else:
            timeout = ctypes.pointer(_timespec(timeout[0], timeout[1]))

    nchanges = len(changelist or ())
    changelist = nchanges and (Event * nchanges)(*changelist) or None
    eventlist = nevents and (Event * nevents)() or None

    r = _libc.kevent(kq, changelist, nchanges, eventlist, nevents, timeout)
    assert r != -1, errno.errorcode[ctypes.get_errno()]

    return [eventlist[i] for i in range(r)]

class kqueue:
    def __init__(self):
        self._kq = _libc.kqueue()
        assert self._kq != -1
    def fileno(self):
        return self._kq
    kevent = kevent
    control = kevent

def selectBroken():
    import socket
    a, b = socket.socketpair()
    kq = select.kqueue()
    try:
        a.send('a')
        b.send('b')
        event1 = select.kevent(a.fileno(), select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_ENABLE)
        event2 = select.kevent(b.fileno(), select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_ENABLE)
        r = kq.control([event1, event2], 2, 0)
        return any(i.flags & select.KQ_EV_ERROR for i in r)

    finally:
        a.close()
        b.close()
        kq.close()

if selectBroken():
    select.kevent = Event
    select.kqueue = kqueue

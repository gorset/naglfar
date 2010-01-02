# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#   * Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY ERIK GORSET, AND CONTRIBUTORS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"wrapper for the sendfile call"

import os
import sys
import time
import errno
import ctypes
import ctypes.util
import socket

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_sendfile = _libc.sendfile

class Iovecs(ctypes.Structure):
    _fields_ = [
        ('iov_base', ctypes.c_void_p),
        ('iov_len', ctypes.c_size_t)
    ]

class SfHdtr(ctypes.Structure):
    _fields_ = [
        ('headers', ctypes.POINTER(Iovecs)),
        ('hdr_cnt', ctypes.c_int),
        ('trailers', ctypes.POINTER(Iovecs)),
        ('trl_cnt', ctypes.c_int)
    ]
    
    @classmethod
    def make(cls, headers, trailers):
        a = (Iovecs*len(headers))(*list(Iovecs(ctypes.cast(i, ctypes.c_void_p), len(i)) for i in headers)) if headers else None
        b = (Iovecs*len(trailers))(*list(Iovecs(ctypes.cast(i, ctypes.c_void_p), len(i)) for i in trailers)) if trailers else None
        return SfHdtr(a, len(headers), b, len(trailers)) if headers or trailers else None

if sys.platform == 'darwin':
    # osx
    # sendfile(int fd, int s, off_t offset, off_t *len, struct sf_hdtr *hdtr, int flags);
    _sendfile.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(SfHdtr), ctypes.c_int]

    def sendfile(fd, s, offset, nbytes, headers=None, trailers=None):
        if headers: # darwin has some kinks
            nbytes += sum(len(i) for i in headers)
        x = ctypes.c_uint64(nbytes)
        t = time.time()
        r = _sendfile(fd, s, offset, x, SfHdtr.make(headers, trailers), 0)
        if r == -1:
            number = ctypes.get_errno()
            if number == errno.EAGAIN:
                return x.value
            raise OSError(number, os.strerror(number))
        return x.value

else:
    # freebsd
    # sendfile(int fd, int s, off_t offset, size_t nbytes, struct sf_hdtr *hdtr, off_t *sbytes, int flags);
    _sendfile.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_size_t, ctypes.POINTER(SfHdtr), ctypes.POINTER(ctypes.c_uint64), ctypes.c_int]

    def sendfile(fd, s, offset, nbytes, headers=None, trailers=None):
        x = ctypes.c_uint64()
        r = _sendfile(fd, s, offset, nbytes, SfHdtr.make(headers, trailers), x, 0)
        if r == -1:
            number = ctypes.get_errno()
            if number == errno.EAGAIN:
                return x.value
            raise OSError(number, os.strerror(number))
        return x.value

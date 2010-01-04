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
"""
from core import go, goRead, goWrite, goClose, Channel, ScheduledFile, ScheduledMixIn, scheduler, queue, testScheduledServer

import objects

class ObjectFile(ScheduledFile):
    def readObject(self):
        headerData = self.read(objects.headerSize)
        assert len(headerData) == objects.headerSize
        header = objects.parseHeader(headerData)
        data = self.read(header.length)
        assert len(data) == header.length
        return objects.loads(headerData + data)

    def writeObject(self, obj):
        self.write(objects.dumps(obj))

    def readObjectStream(self):
        while True:
            try:
                yield self.readObject()
            except:
                # FIXME: check exceptions
                return


__all__ = 'go, goRead, goWrite, goClose, Channel, ScheduledFile, ScheduledMixIn, scheduler, queue, testScheduledServer, objects, ObjectFile'.split(', ')

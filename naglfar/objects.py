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

"serialization of some python objects"

import struct
from collections import namedtuple
from itertools import count, chain

TYPE_TUPLE = 0
TYPE_BYTES = 1
TYPE_INTEGER = 2

def bytesToInt(data):
    n = sum((ord(i) << (8*pos)) for pos, i in enumerate(data))
    return -(n >> 1) if n & 1 else n >> 1

def intToBytes(n):
    if n < 0:
        n = (-n << 1) + 1
    else:
        n <<= 1
    b = bytearray()
    while n:
        b.append(n & 255)
        n >>= 8
    return str(b)

def load(stream):
    objects = {}
    deferred = {}
    for x, y, z in stream:
        if y in (TYPE_INTEGER, TYPE_BYTES):
            objects[x] = z
        else:
            assert y == TYPE_TUPLE
            deferred[x] = z

    def get(identity):
        if identity in objects:
            return objects[identity]

        references = deferred[identity]
        iterator = iter(references)
        t = objects[iterator.next()]
        if t == 'tuple':
            objects[identity] = data = tuple(get(i) for i in iterator)
        elif t == 'dict':
            objects[identity] = data = {}
            data.update((get(i), get(iterator.next())) for i in iterator)
        elif t == 'list':
            objects[identity] = data = []
            data.extend(get(i) for i in iterator)
        elif t == 'unicode':
            objects[identity] = data = unicode(get(iterator.next()), 'UTF-8')
        else:
            assert False, (identity, t, references)

        return data

    return get(0)

def dump(root, objects=None, idMap=None, ids=None):
    if objects is None:
        objects = {}
    if idMap is None:
        idMap = {}
    if ids is None:
        ids = count()

    def getIdentity(obj):
        try:
            if type(obj) == unicode:
                raise TypeError('boink')
            hash(obj)
        except TypeError:
            try:
                return idMap[id(obj)]
            except KeyError:
                idMap[id(obj)] = identity = ids.next()
                return identity
            """
            unstable id support
            for id, value in idMap.items():
                if value is obj:
                    return id
            else:
                id = ids.next()
                idMap[id] = obj
                return id
            """
        else:
            if obj not in objects:
                objects[obj] = ids.next()
            return objects[obj]

    q = [root]
    done = set()
    while q:
        obj = q.pop()
        identity = getIdentity(obj)
        if identity in done:
            continue
        done.add(identity)

        if type(obj) in (int, long):
            yield identity, TYPE_INTEGER, obj
        elif type(obj) == bytes:
            yield identity, TYPE_BYTES, obj
        else:
            if type(obj) == dict:
                values = ('dict', ) + tuple(chain(*obj.items()))
            elif type(obj) == list:
                values = ('list', ) + tuple(obj)
            elif isinstance(obj, tuple):
                values = ('tuple',) + obj
            elif type(obj) == unicode:
                values = ('unicode', obj.encode('UTF-8'))
            else:
                raise NotImplementedError('unsupported object: %s %s' % (obj, type(obj)))

            q.extend(values)
            yield identity, TYPE_TUPLE, tuple(getIdentity(i) for i in values)

def marshal(stream):
    for id, t, data in stream:
        raw = marshalData(t, data)
        yield struct.pack('%s%ss' % (headerFormat, len(raw)), id, t, len(raw), raw)

def marshalData(t, data):
    if t == TYPE_BYTES:
        return data
    elif t == TYPE_INTEGER:
        return intToBytes(data)
    elif t == TYPE_TUPLE:
        return ''.join(struct.pack('>' + 'L'*len(data), *data))
    else:
        assert 0, (t, data)

headerFormat = '>LBL'
headerSize = struct.calcsize(headerFormat)
Header = namedtuple('Header', 'id type length')

def parseHeader(data):
    return Header._make(struct.unpack_from(headerFormat, data))

def unmarshal(stream):
    stream = iter(stream)
    buf = bytearray()
    while True:
        while len(buf) < headerSize:
            buf += stream.next()

        header = parseHeader(str(buf[:headerSize]))
        while len(buf) < header.length + headerSize:
            buf += stream.next()

        data = unmarshalData(header.type, str(buf), headerSize, header.length)
        del buf[:headerSize+header.length]
        yield header.id, header.type, data

def unmarshalData(type, block, offset, size):
    assert len(block) >= offset + size, (len(block), offset, size)
    if type == TYPE_BYTES:
        return block[offset:offset+size]
    elif type == TYPE_INTEGER:
        return bytesToInt(block[offset:offset+size])
    elif type == TYPE_TUPLE:
        assert not size % 4
        return tuple(struct.unpack_from('>' + 'L'*(size / 4), block, offset))
    else:
        assert 0, type

def dumpstream(stream):
    for i in stream:
        data = ''.join(marshal(dump(i)))
        for j in marshal([(0, TYPE_BYTES, data)]):
            yield j

def loadstream(stream):
    for id, t, data in unmarshal(stream):
        assert id == 0 and t == TYPE_BYTES
        yield load(unmarshal([data]))

def dumps(obj):
    return ''.join(dumpstream([obj]))

def loads(s):
    obj, = loadstream([s])
    return obj

if __name__ == '__main__':
    import sys
    filename = sys.argv[1]
    for i in loadstream(open(filename).read()):
        print '--', [i]

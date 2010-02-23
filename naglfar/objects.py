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
import binascii
from itertools import count, chain
from collections import namedtuple

TYPE_TUPLE = 0
TYPE_BYTES = 1
TYPE_INTEGER = 2

Header = namedtuple('Header', 'id type length')
PreHeader = namedtuple('PreHeader', 'id_size length_size type')
Element = namedtuple('Element', 'id type data')

# id
# (id_size:3)(length_size:3)(type:2)(id:id_size<<2)(length:length_size<<2)

def unpackHeader1(b):
    object_type = b & 3
    length_size = b & 28 or 32
    id_size     = (b & 224) >> 3 or 32
    if (id_size + length_size) & 7:
        id_size += 2
        length_size += 2
    return PreHeader(id_size, length_size, object_type)

def unpackHeader2(data, id_size, length_size, offset=0):
    assert id_size + length_size <= 64
    if len(data) - offset < 8:
        data = str(data[offset:]) + '\x00\x00\x00\x00\x00\x00\x00\x00'
        offset = 0
    n, = struct.unpack('>Q', str(data[offset:offset+8]))
    return n>>(64-id_size), (n>>(64-id_size-length_size)) & ((1<<length_size)-1)

table = [0, 1, None, 16, None, 2, 29, None, 17, None, None, None, 3, 22, 30, None, None, None, 20, 18, 11, None, 13, None, None, 4, None, 7, None, 23, 31, None, 15, None, 28, None, None, None, 21, None, 19, 10, 12, None, 6, None, None, 14, 27, None, None, 9, None, 5, None, 26, None, 8, 25, None, 24, None, 32, None]

def nlz(x):
    x |= (x >> 1)
    x |= (x >> 2)
    x |= (x >> 4)
    x |= (x >> 8)
    x |= (x >> 16)
    x *= 0x06EB14F9
    x &= 4294967295
    return table[x >> 26]

def marshalHeader(id, type, length):
    assert type <= 3 and type >= 0
    id_size = nlz(id)
    length_size = nlz(length)

    if id_size < 4:
        id_size = 4
    if length_size < 4:
        length_size = 4
    if id_size & 1:
        id_size += 1
    if length_size & 1:
        length_size += 1

    if id_size & 3:
        if not length_size & 3:
            id_size += 2
    elif length_size & 3:
        length_size += 2

    if (id_size + length_size) & 7:
        id_size += 2
        length_size += 2

    n = 0
    n |= id<<(64-id_size) 
    n |= length<<(64-id_size-length_size) 
    return chr(((id_size<<3)&224) | (length_size&28) | type) + struct.pack('>Q', n)[:(id_size+length_size) >> 3]

def parseHeader(data):
    id_size, length_size, type = unpackHeader1(ord(data[0]))
    id, length = unpackHeader2(data, id_size, length_size, 1)
    return Header(id, type, length)

def bytesToInt(data):
    n = sum((ord(i) << (8*pos)) for pos, i in enumerate(data))
    return -(n >> 1) if n & 1 else n >> 1

def intToBytes(n):
    if not n:
        return ''
    elif n < 0:
        h = hex(((-n)<<1) | 1)
    else:
        h = hex(n<<1)

    if h.endswith('L'):
        h = h[2:-1]
    else:
        h = h[2:]

    if len(h) & 1:
        h = '0' + h
    return binascii.unhexlify(h)[::-1]

def load(stream):
    objects = {}
    deferred = {}
    for i in stream:
        if i.type in (TYPE_INTEGER, TYPE_BYTES):
            objects[i.id] = i.data
        else:
            assert i.type == TYPE_TUPLE
            deferred[i.id] = i.data

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
        elif t == 'set':
            objects[identity] = data = set()
            data.update(get(i) for i in iterator)
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
            if isinstance(obj, dict):
                values = ('dict', ) + tuple(chain(*obj.items()))
            elif type(obj) == list:
                values = ('list', ) + tuple(obj)
            elif isinstance(obj, tuple):
                values = ('tuple',) + obj
            elif type(obj) == set:
                values = ('set', ) + tuple(obj)
            elif type(obj) == unicode:
                values = ('unicode', obj.encode('UTF-8'))
            else:
                raise NotImplementedError('unsupported object: %s %s' % (obj, type(obj)))

            q.extend(values)
            yield identity, TYPE_TUPLE, tuple(getIdentity(i) for i in values)

def marshal(stream):
    for id, t, data in stream:
        raw = marshalData(t, data)
        yield marshalHeader(id, t, len(raw))
        yield raw
        #yield struct.pack('%s%ss' % (headerFormat, len(raw)), id, t, len(raw), raw)

def marshalData(t, data):
    if t == TYPE_BYTES:
        return data
    elif t == TYPE_INTEGER:
        return intToBytes(data)
    elif t == TYPE_TUPLE:
        if not data:
            return ''
        n = max(data)
        if n<256:
            h = '\x00'
            format = 'B'
        elif n<65536:
            h = '\x01'
            format = 'H'
        elif n<4294967296:
            h = '\x02'
            format = 'I'
        else:
            assert False
        return h + ''.join(struct.pack('>' + format*len(data), *data))
    else:
        assert 0, (t, data)

#def parseHeader(data):
#    return Header._make(struct.unpack_from(headerFormat, data))

def unmarshal(stream):
    stream = iter(stream)
    buffer = bytearray()
    while True:
        if not buffer:
            buffer += stream.next()

        id_size, length_size, type = unpackHeader1(buffer[0])
        headerSize = 1 + ((id_size+length_size)>>3)
        while len(buffer) < headerSize:
            buffer += stream.next()

        id, length = unpackHeader2(buffer, id_size, length_size, 1)

        while len(buffer) < headerSize + length:
            buffer += stream.next()

        data = unmarshalData(type, str(buffer), headerSize, length)
        del buffer[:headerSize+length]
        yield Element(id, type, data)

def unmarshalData(type, block, offset, size):
    assert len(block) >= offset + size, (len(block), offset, size)
    if type == TYPE_BYTES:
        return block[offset:offset+size]
    elif type == TYPE_INTEGER:
        return bytesToInt(block[offset:offset+size])
    elif type == TYPE_TUPLE:
        if not size:
            return ()
        h = ord(block[offset])
        size -= 1
        if h == 0:
            format = 'B'*size
        elif h == 1:
            assert not size & 1
            format = 'H'*(size/2)
        elif h == 2:
            assert not size & 3, size
            format == 'I'*(size/4)
        return tuple(struct.unpack_from('>' + format, block, offset+1))
    else:
        assert 0, type

def dumpstream(stream):
    for i in stream:
        data = ''.join(marshal(dump(i)))
        for j in marshal([(0, TYPE_BYTES, data)]):
            yield j

def loadstream(stream):
    for i in unmarshal(stream):
        assert i.id == 0 and i.type == TYPE_BYTES
        yield load(unmarshal([i.data]))

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

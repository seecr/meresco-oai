## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
#
# This file is part of "Meresco Oai"
#
# "Meresco Oai" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Meresco Oai" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Meresco Oai"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from os.path import join, isdir, getsize
from os import listdir, makedirs, SEEK_CUR
from escaping import escapeFilename
from zlib import compress, decompress, error as ZlibError
from math import ceil
import operator
from meresco.core import asyncnoreturnvalue

SENTINEL = "----"
RECORD = "%(sentinel)s\n%(key)s\n%(length)s\n%(data)s\n"
LARGER_THAN_ANY_INT = 2**64

class SequentialMultiStorage(object):
    def __init__(self, path):
        self._path = path
        isdir(self._path) or makedirs(self._path)
        self._storage = {}
        for name in listdir(path):
            self._getStorage(name)

    def _getStorage(self, name):
        if name not in self._storage:
            name = escapeFilename(name)
            self._storage[name] = SequentialStorage(join(self._path, name))
        return self._storage[name]

    @asyncnoreturnvalue
    def add(self, identifier, partname, data):
        self.addData(key=identifier, name=partname, data=data)

    def addData(self, key, name, data):
        self._getStorage(name).add(key, data)

    def getData(self, key, name):
        return self._getStorage(name)[key]

    def iterData(self, name, start, stop=LARGER_THAN_ANY_INT, **kwargs):
        return self._getStorage(name).iter(start, stop, **kwargs)

    def handleShutdown(self):
        print 'handle shutdown: saving SequentialMultiStorage %s' % self._path
        from sys import stdout; stdout.flush()
        self.flush()

    def flush(self):
        for storage in self._storage.itervalues():
            storage.flush()


class SequentialStorage(object):
    def __init__(self, fileName, blockSize=8192):
        self._f = open(fileName, "ab+")
        self._blkIndex = _BlkIndex(self, blockSize)
        self._lastKey = None
        lastBlk = self._blkIndex.search(LARGER_THAN_ANY_INT)
        try:
            self._lastKey = self._blkIndex.scan(lastBlk, last=True)
        except StopIteration:
            pass

    def add(self, key, data):
        _intcheck(key)
        if key <= self._lastKey:
            raise ValueError("key %s must be greater than last key %s" % (key, self._lastKey))
        self._lastKey = key
        data = compress(data)
        record = RECORD % dict(key=key, length=len(data), data=data, sentinel=SENTINEL)
        self._f.write(record)
        self._blkIndex._size += len(record)

    def __getitem__(self, key):
        _intcheck(key)
        blk = self._blkIndex.search(key)
        try:
            found_key, data = self._blkIndex.scan(blk, target_key=key)
        except StopIteration:
            raise IndexError
        return data

    def isEmpty(self):
        return len(self._blkIndex) == 0

    def flush(self):
        self._f.flush()

    def _readNext(self, target_key=None, greater=False, keyOnly=False, last=False):
        line = "sentinel not yet found"
        key = None; data = None
        while line != '':
            line = self._f.readline()
            retryPosition = self._f.tell()
            if line.endswith(SENTINEL + '\n'):
                data = None
                try:
                    key = int(self._f.readline().strip())
                    length = int(self._f.readline().strip())
                except ValueError:
                    self._f.seek(retryPosition)
                    continue
                if target_key:
                    if key < target_key:
                        continue
                    elif not greater and key != target_key:
                        raise StopIteration
                if keyOnly:
                    return key
                rawdata = self._f.read(length)
                try:
                    data = decompress(rawdata)
                except ZlibError:
                    self._f.seek(retryPosition)
                    continue
                retryPosition = self._f.tell()
                expectingNewline = self._f.read(1)  # newline after data
                if expectingNewline != '\n':
                    self._f.seek(retryPosition)
                if last:
                    continue
                return key, data
        if last and key and data:
            return key
        raise StopIteration

    def iter(self, start, stop=LARGER_THAN_ANY_INT, inclusive=False):
        _intcheck(start); _intcheck(stop)
        cmp = operator.le if inclusive else operator.lt
        blk = self._blkIndex.search(start)
        key, data = self._blkIndex.scan(blk, target_key=start, greater=True)
        offset = self._f.tell()
        while cmp(key, stop):
            yield key, data
            self._f.seek(offset)
            key, data = self._readNext()
            offset = self._f.tell()
            
class _BlkIndex(object):

    def __init__(self, src, blk_size):
        self._src = src
        self._blk_size = blk_size
        self._cache = {}
        self._size = getsize(src._f.name)

    def __getitem__(self, blk):
        key = self._cache.get(blk)
        if not key:
            try:    
                key = self._cache[blk] = self.scan(blk, keyOnly=True)
            except StopIteration:
                raise IndexError
        return key

    def __len__(self):
        return ceil(self._size / float(self._blk_size))

    def scan(self, blk, **kwargs):
        self._src._f.seek(blk * self._blk_size)
        return self._src._readNext(**kwargs)

    def search(self, key):
        return max(_bisect_left(self, key)-1, 0)


def _intcheck(value):
    if not isinstance(value, (int, long)):
        raise ValueError('Expected int')


# from Python lib
def _bisect_left(a, x, lo=0, hi=None):
    """Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
    insert just before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """

    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo+hi)//2
        try: # EG
            if a[mid] < x:
                lo = mid+1
            else: hi = mid
        except IndexError: #EG
            hi = mid #EG
    return lo

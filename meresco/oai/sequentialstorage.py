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

from os.path import join, isdir
from os import listdir, makedirs
from collections import OrderedDict
from escaping import escapeFilename
from zlib import compress, decompress
from meresco.core import asyncnoreturnvalue


SENTINEL = "----"
RECORD = "%(sentinel)s\n%(key)s\n%(length)s\n%(data)s\n"
BLOCKSIZE = len(RECORD % dict(sentinel=SENTINEL, key=1, length=1, data="1"))
DEFAULT_CACHESIZE = 100000

class KeyIndex(object):

    def __init__(self, src, maxSize):
        self._src = src
        self._cache = OrderedDict()
        self._maxSize = maxSize

    def __len__(self):
        return len(self._src)

    def __getitem__(self, key):
        if key in self._cache:
            result = self._cache.pop(key)
            self._cache[key] = result
            return result
        index = self._src[key][0]
        self._cache[key] = index
        if len(self._cache) > self._maxSize:
            self._cache.popitem(0)
        return index


class Iter(object):

    def __init__(self, src, start, stop, inclusive=False):
        self._offset = BLOCKSIZE * bisect_left(src._index, start)
        self._src = src
        if stop:
            if inclusive:
                self._shouldStop = lambda key: key > stop
            else:
                self._shouldStop = lambda key: key >= stop
        else:
            self._shouldStop = lambda key: False

    def next(self):
        self._src._f.seek(self._offset)
        key, data = self._src._readNext()
        self._offset = self._src._f.tell()
        if self._shouldStop(key):
            raise StopIteration
        return key, data

    def __iter__(self):
        return self


class SequentialMultiStorage(object):
    def __init__(self, path, maxCacheSize=DEFAULT_CACHESIZE):
        self._path = path
        isdir(self._path) or makedirs(self._path)
        self._storage = {}
        self._maxCacheSize = maxCacheSize
        for name in listdir(path):
            self._getStorage(name)

    def _getStorage(self, name):
        if name not in self._storage:
            name = escapeFilename(name)
            self._storage[name] = SequentialStorage(join(self._path, name), maxCacheSize=self._maxCacheSize)
        return self._storage[name]

    @asyncnoreturnvalue
    def add(self, identifier, partname, data):
        self.addData(key=identifier, name=partname, data=data)

    def addData(self, key, name, data):
        self._getStorage(name).add(key, data)

    def getData(self, key, name):
        return self._getStorage(name).index(key)

    def iterData(self, name, start, stop, **kwargs):
        return self._getStorage(name).iter(start, stop, **kwargs)

    def handleShutdown(self):
        print 'handle shutdown: saving SequentialMultiStorage %s' % self._path
        from sys import stdout; stdout.flush()
        self.flush()

    def flush(self):
        for storage in self._storage.itervalues():
            storage.flush()

def _intcheck(value):
    if type(value) is not int:
        raise ValueError('Expected int')

class SequentialStorage(object):
    def __init__(self, fileName, maxCacheSize):
        self._f = open(fileName, "ab+")
        self._index = KeyIndex(self, maxSize=maxCacheSize)
        if len(self):
            lastindex = bisect_left(self._index, "zzzzzzzzzzz")
            self._lastKey = self[lastindex - 1][0]
        else:
            self._lastKey = None

    def add(self, key, data):
        _intcheck(key)
        if key <= self._lastKey:
            raise ValueError("key %s must be greater than last key %s" % (key, self._lastKey))
        self._lastKey = key
        sentinel = SENTINEL
        data = compress(data)
        length = len(data)
        record = RECORD % locals()
        self._f.write(record) # one write is a little bit faster

    def flush(self):
        self._f.flush()

    def __len__(self):
        self._f.seek(0, 2)
        return self._f.tell() / BLOCKSIZE

    def _readNext(self):
        sentinel = "not yet found"
        while sentinel != '':
            sentinel = self._f.readline()
            if sentinel.strip() == SENTINEL:
                identifier = int(self._f.readline().strip())
                try:
                    length = int(self._f.readline().strip())
                except ValueError:
                    continue
                data = self._f.read(length)
                assert len(data) == length
                return identifier, decompress(data)
        raise StopIteration

    def __getitem__(self, key):
        _intcheck(key)
        self._f.seek(key * BLOCKSIZE)
        try:
            return self._readNext()
        except StopIteration:
            raise IndexError

    def index(self, key):
        _intcheck(key)
        i = bisect_left(self._index, key)
        found_key, data = self[i]
        if found_key != key:
            raise IndexError
        return data

    def iter(self, start, stop=None, **kwargs):
        _intcheck(start)
        stop is None or _intcheck(stop)
        return Iter(self, start, stop, **kwargs)

# from Python lib
def bisect_left(a, x, lo=0, hi=None):
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

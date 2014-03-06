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
from os import listdir, makedirs, SEEK_CUR, SEEK_END
from escaping import escapeFilename
from zlib import compress, decompress, error as ZlibError
import operator
from array import array
from meresco.core import asyncnoreturnvalue
from ordereddict import OrderedDict


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

    def iterData(self, name, start, stop, **kwargs):
        return self._getStorage(name).iter(start, stop, **kwargs)

    def handleShutdown(self):
        print 'handle shutdown: saving SequentialMultiStorage %s' % self._path
        from sys import stdout; stdout.flush()
        self.flush()

    def flush(self):
        for storage in self._storage.itervalues():
            storage.flush()


class SequentialStorage(object):
    def __init__(self, fileName, cutoff=128):
        self._f = open(fileName, "ab+")
        self._index = _KeyIndex(_BlkIndex(self), cutoff)
        self._lastKey = None
        positionAfterLast = 0
        if self._index:
            positionAfterLast = self._index.find_blk(LARGER_THAN_ANY_INT, cutoff=0)
        if positionAfterLast > 0:
            self._lastKey = self._index[positionAfterLast - 1]

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

    def __getitem__(self, key):
        _intcheck(key)
        i = self._index.find_blk(key)  # FIXME: Zonder cutoff=0 ?  --> Think
        found_key, data = self._keyData(i, key)
        if found_key != key:
            raise IndexError
        return data

    def isEmpty(self):
        return getsize(self._f.name) == 0

    def iter(self, start, stop=None, **kwargs):
        _intcheck(start)
        stop is None or _intcheck(stop)
        return _Iter(self, start, stop, **kwargs)

    def flush(self):
        self._f.flush()

    def _keyData(self, i, key=None):
        self._f.seek(i * BLOCKSIZE)
        try:
            return self._readNext(key)
        except StopIteration:
            raise IndexError

    def _readNext(self, target_key=None):
        line = "sentinel not yet found"
        #nextLineMustBeSentinel = False
        while line != '':
            line = self._f.readline()
            #if nextLineMustBeSentinel:
            #    if not line == SENTINEL + '\n':  # Exact, not endswith.
            #        self._f.seek(retryPosition)
            #        line = self._f.readline()
            #    nextLineMustBeSentinel = False
            retryPosition = self._f.tell()
            if line.endswith(SENTINEL + '\n'):
                try:
                    key = int(self._f.readline().strip())
                    length = int(self._f.readline().strip())
                except ValueError:
                    self._f.seek(retryPosition)
                    continue
                if target_key and key != target_key:  # FIXME: testme
                    self._f.seek(length + 1, SEEK_CUR)
                    ## Iff failing test, try doing:
                    # nextLineMustBeSentinel = True
                    continue
                data = self._f.read(length)
                try:
                    data = decompress(data)
                except ZlibError:
                    self._f.seek(retryPosition)
                    continue
                retryPosition = self._f.tell()
                expectingNewline = self._f.read(1)  # newline after data
                if expectingNewline != '\n':
                    self._f.seek(retryPosition)
                return key, data
        raise StopIteration


class _Iter(object):
    def __init__(self, src, start, stop, inclusive=False):
        self._offset = BLOCKSIZE * src._index.find_blk(start, cutoff=0)
        self._src = src
        self._cmp = operator.gt if inclusive else operator.ge
        self._stop = stop

    def next(self):
        self._src._f.seek(self._offset)
        key, data = self._src._readNext()
        self._offset = self._src._f.tell()
        if self._stop and self._cmp(key, self._stop):
            raise StopIteration
        return key, data

    def __iter__(self):
        return self

SENTINEL = "----"
RECORD = "%(sentinel)s\n%(key)s\n%(length)s\n%(data)s\n"
BLOCKSIZE = len(RECORD % dict(sentinel=SENTINEL, key=1, length=1, data="1"))
LARGER_THAN_ANY_INT = 2**64-1  # see array type  # FIXME: testme!


class _BlkIndex(object):
    """Please keep compatible with Python list in order to simplify testing"""

    def __init__(self, src):
        self._src = src

    def __getitem__(self, blk):
        return self._src._keyData(blk)[0]

    def __len__(self):
        self._src._f.seek(0, SEEK_END)
        return self._src._f.tell() / BLOCKSIZE

class _MemIndex(object):
    def __init__(self):
        self._cache_key = array("L")
        self._cache_blk = array("L")
        assert self._cache_key.itemsize == 8, '64-bits architecture required.'

    def __len__(self):
        return len(self._cache_key)

    def find(self, key):
        lo = _bisect_left(self._cache_key, key)
        lo_blk, hi_blk = 0, None  # TS: TODO: hi_blk kan None zijn, zinnig/nuttig?
        if lo < len(self._cache_key):
            hi_blk = self._cache_blk[lo]
        if lo > 0:
            lo_blk = self._cache_blk[lo-1]
        return lo_blk, hi_blk

    def add(self, key, blk):
        lo = _bisect_left(self._cache_key, key)
        if lo < len(self._cache_key):
            found_key = self._cache_key[lo]
            if found_key == key:
                return self
        self._cache_key.insert(lo, key)
        self._cache_blk.insert(lo, blk)
        return self
        
class _KeyIndex(object):

    def __init__(self, blk, cutoff=0):
        self._cutoff = cutoff
        self._blk = blk
        self._memIndex = _MemIndex()

    def __len__(self):
        return len(self._blk)

    def __getitem__(self, blk):
        key = self._blk[blk]
        self._memIndex.add(key, blk)
        return key

    def find_blk(self, key, cutoff=None):
        lo_blk, hi_blk = self._memIndex.find(key)
        cutoff = cutoff if cutoff != None else self._cutoff
        return _bisect_left(self, key, cutoff=cutoff, lo=lo_blk, hi=hi_blk)

def _intcheck(value):
    if type(value) is not int:
        raise ValueError('Expected int')

# from Python lib
def _bisect_left(a, x, lo=0, hi=None, cutoff=0):
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
    while lo < hi - cutoff:  # EG added cutoff
        mid = (lo+hi)//2
        try: # EG
            if a[mid] < x:
                lo = mid+1
            else: hi = mid
        except IndexError: #EG
            hi = mid #EG
    return lo

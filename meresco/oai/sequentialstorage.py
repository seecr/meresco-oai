from os.path import join
from os import listdir
from collections import defaultdict
from escaping import escapeFilename
from zlib import compress, decompress

SENTINEL = "----\n"
BLOCKSIZE = len(SENTINEL) + 8 + 4 # min length guaranteed not to skip records

class KeyIndex(object):

    def __init__(self, src):
        self._src = src

    def __len__(self):
        return len(self._src)

    def __getitem__(self, i):
        return self._src[i][0]


class Iter(object):

    def __init__(self, src, start, stop):
        self._fp = BLOCKSIZE * bisect_left(src._index, str(start))
        self._src = src
        self._stop = str(stop)

    def next(self):
        self._src._f.seek(self._fp)
        key, data = self._src._readNext()
        self._fp = self._src._f.tell()
        if self._stop and key > self._stop:
            raise StopIteration
        return key, data

    def __iter__(self):
        return self


class SequentialMultiStorage(object):
    def __init__(self, path):
        self._path = path
        self._storage = {}
        for name in listdir(path):
            self._getStorage(name)

    def _getStorage(self, name):
        if name not in self._storage:
            name = escapeFilename(name)
            self._storage[name] = SequentialStorage(join(self._path, name))
        return self._storage[name]

    def add(self, key, name, data):
        self._getStorage(name).add(key, data)

    def get(self, key, name):
        return self._getStorage(name).index(key)

    def iterData(self, name, start, stop):
        return self._getStorage(name).iter(start, stop)

    def flush(self):
        for storage in self._storage.itervalues():
            storage.flush()

class SequentialStorage(object):
    def __init__(self, fileName):
        self._f = open(fileName, "ab+")
        self._index = KeyIndex(self)
        if len(self):
            lastindex = bisect_left(self._index, "zzzzzzzzzzz")
            self._lastKey = self[lastindex - 1][0]
        else:
            self._lastKey = None

    def add(self, key, data):
        key = str(key)
        if key <= self._lastKey:
            raise ValueError("key %s must be greater than last key %s" % (key, self._lastKey))
        self._lastKey = key
        self._f.write(SENTINEL)
        self._f.write("%s\n" % key)
        data = compress(data)
        self._f.write("%s\n" % len(data))
        self._f.write(data + '\n')

    def flush(self):
        self._f.flush()

    def __len__(self):
        self._f.seek(0, 2)
        return self._f.tell() / BLOCKSIZE

    def _readNext(self):
        sentinel = "not yet found"
        while sentinel != '':
            sentinel = self._f.readline()
            if sentinel == SENTINEL:
                identifier = self._f.readline().strip()
                try:
                    length = int(self._f.readline().strip())
                except ValueError:
                    continue
                data = self._f.read(length)
                assert len(data) == length
                return identifier, decompress(data)
        raise StopIteration

    def __getitem__(self, i):
        self._f.seek(i * BLOCKSIZE)
        try:
            return self._readNext()
        except StopIteration:
            raise IndexError

    def index(self, key):
        key = str(key)
        i = bisect_left(self._index, key)
        found_key, data = self[i]
        if found_key != key:
            raise IndexError
        return data

    def iter(self, start, stop=None):
        return Iter(self, start, stop)

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

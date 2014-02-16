from os.path import join
from os import listdir
from collections import defaultdict

SENTINEL = "-------\n"

class Key(object):
    def __init__(self, src):
        self._src = src
    def __len__(self):
        return len(self._src)
    def __getitem__(self, i):
        return self._src[i][0]

class SequentialMultiStorage(object):
    def __init__(self, path):
        self._path = path
        self._storage = {}
        for name in listdir(path):
            self._getStorage(name)

    def _getStorage(self, name):
        if name not in self._storage:
            self._storage[name] = SequentialStorage(join(self._path, name))
        return self._storage[name]

    def add(self, key, name, data):
        self._getStorage(name).add(key, data)

    def get(self, key, name):
        return self._getStorage(name).index(key)

class SequentialStorage(object):
    def __init__(self, fileName):
        self._f = open(fileName, "a+")
        self._lastKey = None
        self._index = Key(self)

    def add(self, key, data):
        if key <= self._lastKey:
            raise ValueError("key %s must be greater than last key %s" 
                % (key, self._lastKey))
        self._lastKey = key
        self._f.write(SENTINEL)
        self._f.write(key + '\n')
        self._f.write(data + '\n')
        self._f.flush()

    def __len__(self):
        self._f.seek(0, 2)
        return self._f.tell() / len(SENTINEL)

    def __getitem__(self, i):
        size = self._f.tell()
        self._f.seek(i * len(SENTINEL))
        sentinel = "not yet found"
        while sentinel != '':
            sentinel = self._f.readline()
            if sentinel == SENTINEL:
                identifier = self._f.readline().strip()
                data = self._f.readline().strip()
                return identifier, data
        raise IndexError

    def index(self, key):
        i = bisect_left(self._index, key)
        return self[i][1]

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

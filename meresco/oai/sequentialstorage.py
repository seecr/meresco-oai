from os.path import join
from os import listdir
from collections import defaultdict

SENTINEL = "-------\n"

class SequentialStorage(object):
    def __init__(self, path):
        self._path = path
        self._streams = {}
        for name in listdir(path):
            self._getStream(name)
        self._lastKeys = defaultdict(int)

    def _getStream(self, name):
        if name not in self._streams:
            self._streams[name] = open(join(self._path, name), "a+")
        return self._streams[name]

    def add(self, key, part, data):
        if key <= self._lastKeys[part]:
            raise ValueError("key %s must be greater than last key %s" 
                % (key, self._lastKeys[part]))
        self._lastKeys[part] = key
        f = self._getStream(part)
        f.write(SENTINEL)
        f.write(key + '\n')
        f.write(data + '\n')
        f.flush()

    def get(self, key, part):
        if part not in self._streams:
            raise KeyError
        f = self._getStream(part)
        f.seek(0)
        while True:
            sentinel = f.readline()
            key_ = f.readline().strip()
            if key_ == '':
                raise KeyError
            data = f.readline().strip()
            if  key_ == key:
                return data

    def __len__(self):
        return self._getStream("oai_dc").tell() / len(SENTINEL)

    def __getitem__(self, i):
        f = self._getStream("oai_dc")
        size = f.tell()
        f.seek(i * len(SENTINEL))
        sentinel = "not yet found"
        while sentinel != '':
            sentinel = f.readline()
            if sentinel == SENTINEL:
                identifier = f.readline().strip()
                data = f.readline().strip()
                return identifier, data
        raise IndexError

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
        if a[mid] < x: lo = mid+1
        else: hi = mid
    return lo

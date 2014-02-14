from os.path import join
from os import listdir
from collections import defaultdict


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
        f.write(key + '\n')
        f.write(data + '\n')
        f.flush()

    def get(self, key, part):
        if part not in self._streams:
            raise KeyError
        f = self._getStream(part)
        f.seek(0)
        while True:
            key_ = f.readline().strip()
            if key_ == '':
                raise KeyError
            data = f.readline().strip()
            if  key_ == key:
                return data

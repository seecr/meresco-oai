## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from hashlib import sha1
from math import ceil

class Partition(object):
    NR_OF_PARTS = 1024

    def __init__(self, parts, total):
        if total < 2:
            raise ValueError('Expected at least 2 partitions.')
        if total > 10:
            raise ValueError('Expected max 10 partitions.')
        if not parts or not set(parts).issubset(set(xrange(1, total+1))):
            raise ValueError('Expected parts >= 1 and <= {0}.'.format(total))
        self._parts = parts
        self._total = total

    @classmethod
    def create(cls, aString):
        if not aString:
            return None
        aString = str(aString)
        parts, total = aString.split('/')
        return cls(
            parts=[int(p) for p in parts.split(',')],
            total=int(total),
        )

    @classmethod
    def hashId(cls, identifier):
        return int(int(sha1(identifier).hexdigest(),16) % cls.NR_OF_PARTS)

    def ranges(self):
        partSize = int(ceil(self.NR_OF_PARTS / float(self._total)))
        return ((start*partSize, end*partSize) for start, end in self._ranges())

    def _ranges(self):
        lastEnd = self._parts[0]
        lastStart = lastEnd -1
        for end in self._parts[1:]:
            start = end -1
            if start == lastEnd:
                lastEnd = end
            else:
                yield (lastStart, lastEnd)
                lastStart, lastEnd = start, end
        yield (lastStart, lastEnd)


    def __str__(self):
        return "{0}/{1}".format(
                ','.join(str(p) for p in self._parts),
                self._total,
            )

    def __eq__(self, other):
        return \
            self.__class__ == other.__class__ and \
            self._parts == other._parts and \
            self._total == other._total

    def __hash__(self):
        return hash(str(self)) + hash(self.__class__)

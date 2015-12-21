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

class PartHash(object):
    NR_OF_PARTS = 1024
    ALLOWED = ['1/2', '2/2']

    def __init__(self, parts, total):
        self.parts = parts
        self.partSize = self.NR_OF_PARTS / total

    @classmethod
    def fromString(cls, aString):
        if not aString:
            return None
        aString = str(aString)
        if aString not in cls.ALLOWED:
            raise ValueError("Partition not allowed.")
        parts, total = aString.split('/')
        return cls(
            parts=[int(p)-1 for p in parts.split(',')],
            total=int(total),
        )

    @classmethod
    def hashId(cls, identifier):
        return int(int(sha1(identifier).hexdigest(),16) % cls.NR_OF_PARTS)

    def ranges(self):
        for part in self.parts:
            yield part*self.partSize, (part+1)*self.partSize

    def __str__(self):
        return "{0}/{1}".format(
                ','.join(str(p+1) for p in self.parts),
                self.NR_OF_PARTS / self.partSize,
            )

    def __eq__(self, other):
        return \
            PartHash == other.__class__ and \
            self.parts == other.parts and \
            self.partSize == other.partSize

    def __hash__(self):
        return hash(str(self)) + hash(self.__class__)

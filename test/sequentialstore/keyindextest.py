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

from seecr.test import SeecrTestCase

from meresco.oai.sequentialstorage import _KeyIndex


class KeyIndexTest(SeecrTestCase):
    def testMaxSize(self):
        requestedKeys = []
        class Source(object):
            def __getitem__(self, key):
                requestedKeys.append(key)
                return (1000 + key, 'ignored')
        keyIndex = _KeyIndex(Source(), maxSize=3)
        self.assertEquals(1001, keyIndex[1])
        self.assertEquals(1002, keyIndex[2])
        self.assertEquals([1, 2], requestedKeys)
        self.assertEquals(1002, keyIndex[2])
        self.assertEquals(1002, keyIndex[2])
        self.assertEquals([1, 2], requestedKeys)
        self.assertEquals(1003, keyIndex[3])
        self.assertEquals(1004, keyIndex[4])
        self.assertEquals([1, 2, 3, 4], requestedKeys)
        self.assertEquals(1002, keyIndex[2])
        self.assertEquals([1, 2, 3, 4], requestedKeys)
        self.assertEquals(1001, keyIndex[1])
        self.assertEquals([1, 2, 3, 4, 1], requestedKeys)

        self.assertEquals(3, len(keyIndex._cache))
        self.assertEquals([4,2,1], keyIndex._cache.keys())

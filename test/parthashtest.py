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

from seecr.test import SeecrTestCase

from meresco.oai._parthash import PartHash

class PartHashTest(SeecrTestCase):
    def testDisallowed(self):
        self.assertRaises(ValueError, lambda: PartHash.create('1,2/3'))

    def testHash(self):
        self.assertEquals(485, PartHash.hashId("identifier"))
        self.assertEquals(1024, PartHash.NR_OF_PARTS)

    def testRanges(self):
        self.assertEquals([(0,512)], list(PartHash.create('1/2').ranges()))
        self.assertEquals([(512,1024)], list(PartHash.create('2/2').ranges()))

    def testStr(self):
        self.assertEquals("1/2", "%s" % PartHash.create('1/2'))
        self.assertEquals("2/2", str(PartHash.create('2/2')))

    def testEquals(self):
        self.assertEquals(PartHash.create('1/2'), PartHash.create('1/2'))
        self.assertEquals(hash(PartHash.create('1/2')), hash(PartHash([0],2)))

    def testFromStringNone(self):
        self.assertEquals(None, PartHash.create(None))
        self.assertEquals(None, PartHash.create(''))
        self.assertEquals(PartHash.create('1/2'), PartHash.create(PartHash.create('1/2')))

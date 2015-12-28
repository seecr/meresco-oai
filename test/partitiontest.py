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

from meresco.oai._partition import Partition

class PartitionTest(SeecrTestCase):
    def testDisallowed(self):
        self.assertRaises(ValueError, lambda: Partition.create('1,4/3'))
        self.assertRaises(ValueError, lambda: Partition.create('1/30'))

    def testHash(self):
        self.assertEquals(485, Partition.hashId("identifier"))
        self.assertEquals(1024, Partition.NR_OF_PARTS)

    def testRanges(self):
        self.assertEquals([(0,512)], list(Partition.create('1/2').ranges()))
        self.assertEquals([(512,1024)], list(Partition.create('2/2').ranges()))
        self.assertEquals([(0,205)], list(Partition.create('1/5').ranges()))
        self.assertEquals([(820,1025)], list(Partition.create('5/5').ranges()))
        self.assertEquals([(927,1030)], list(Partition.create('10/10').ranges()))
        self.assertEquals([(0,205), (820,1025)], list(Partition.create('1,5/5').ranges()))
        self.assertEquals([(0, 410), (820,1025)], list(Partition.create('1,2,5/5').ranges()))

    def testStr(self):
        self.assertEquals("1/2", "%s" % Partition.create('1/2'))
        self.assertEquals("2/2", str(Partition.create('2/2')))
        self.assertEquals("2/10", str(Partition.create('2/10')))
        self.assertEquals("1/10", str(Partition.create('1/10')))
        self.assertEquals("1,3,4,5/7", str(Partition.create('1,3,4,5/7')))

    def testEquals(self):
        self.assertEquals(Partition.create('1/2'), Partition.create('1/2'))
        self.assertEquals(hash(Partition.create('1/2')), hash(Partition([1],2)))

    def testFromStringNone(self):
        self.assertEquals(None, Partition.create(None))
        self.assertEquals(None, Partition.create(''))
        self.assertEquals(Partition.create('1/2'), Partition.create(Partition.create('1/2')))

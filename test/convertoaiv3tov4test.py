## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from os import system
from os.path import dirname, join, abspath, isdir, isfile
from shutil import copytree
from bsddb import btopen

from seecr.test import SeecrTestCase

mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertOaiV3ToV4Test(SeecrTestCase):
    def testConversion(self):
        datadir = join(self.tempdir, 'oai_conversion_v3_to_v4')
        copytree(join(mypath, 'data', 'oai_conversion_v3_to_v4'), datadir)
        system("%s %s > %s 2>&1" % (
                join(binDir, 'convert_oai_v3_to_v4'),
                datadir,
                join(self.tempdir, 'oai_conversion_v3_to_v4.log'),
            ))

        self.assertFalse(isfile(join(datadir, 'stamp2identifier.bd')))
        self.assertFalse(isfile(join(datadir, 'identifier2setSpecs.bd')))
        identifierDict = btopen(join(datadir, 'stamp2identifier2setSpecs.bd'))

        self.assertEquals({
                "ss:http://zp.seecr.nl/example/will_be_deleted_soon": "testCollection",
                "ss:ggc:GGC:AC:158187709": "testCollection",
                "st:1349938816963267": "http://zp.seecr.nl/example/will_be_deleted_soon",
                "st:id:ggc:GGC:AC:158187709": "1349938816867224",
                "st:id:http://zp.seecr.nl/example/will_be_deleted_soon": "1349938816963267",
                "st:1349938816867224": "ggc:GGC:AC:158187709"
            }, identifierDict)

        self.assertEquals(6, len(identifierDict))

        self.assertTrue(isfile(join(datadir, 'tombStones.list')))
        self.assertTrue(isfile(join(datadir, 'prefixes', 'rdf.list')))
        self.assertTrue(isfile(join(datadir, 'prefixesInfo', 'rdf.schema')))
        self.assertTrue(isfile(join(datadir, 'prefixesInfo', 'rdf.namespace')))
        self.assertTrue(isfile(join(datadir, 'sets', 'testCollection.list')))

        self.assertEquals('4', open(join(datadir, 'oai.version')).read())

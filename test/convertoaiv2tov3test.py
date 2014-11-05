## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2012-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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
from meresco.components import PersistentSortedIntegerList
from meresco.components.integerlist import IntegerList
from meresco.oai4 import OaiJazz

mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertOaiV2ToV3Test(SeecrTestCase):
    def testConversion(self):
        datadir = join(self.tempdir, 'oai_conversion_v2_to_v3')
        copytree(join(mypath, 'data', 'oai_conversion_v2_to_v3'), datadir)
        system("%s %s > %s 2>&1" % (
                join(binDir, 'convert_oai_v2_to_v3'),
                datadir,
                join(self.tempdir, 'oai_conversion_v2_to_v3.log'),
            ))

        self.assertTrue(isfile(join(datadir, 'stamp2identifier.bd')))
        stamp2identifier = btopen(join(datadir, 'stamp2identifier.bd'))
        self.assertEquals(4, len(stamp2identifier))
        for key, value in stamp2identifier.items():
            if key.startswith('id:'):
                stamp, identifier = value, key[len('id:'):]
                self.assertEquals(identifier, stamp2identifier[stamp])
            else:
                stamp, identifier = key, value
                self.assertEquals(stamp, stamp2identifier["id:" + identifier])
        self.assertFalse(isdir(join(datadir, 'stamp2identifier')))

        self.assertTrue(isfile(join(datadir, 'identifier2setSpecs.bd')))
        identifier2setSpecs = btopen(join(datadir, 'identifier2setSpecs.bd')) 
        self.assertEquals(2, len(identifier2setSpecs))
        self.assertFalse(isdir(join(datadir, 'identifier2setSpecs')))

        self.assertTrue(isfile(join(datadir, 'tombStones.list')))
        self.assertTrue(isfile(join(datadir, 'prefixes', 'rdf.list')))
        self.assertTrue(isfile(join(datadir, 'prefixesInfo', 'rdf.schema')))
        self.assertTrue(isfile(join(datadir, 'prefixesInfo', 'rdf.namespace')))
        self.assertTrue(isfile(join(datadir, 'sets', 'testCollection.list')))

        self.assertEquals('3', open(join(datadir, 'oai.version')).read())

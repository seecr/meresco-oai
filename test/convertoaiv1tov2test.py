## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet http://www.kennisnet.nl
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
from os.path import dirname, join, abspath, isdir
from shutil import copytree

from seecr.test import SeecrTestCase
from meresco.components import PersistentSortedIntegerList
from meresco.components.integerlist import IntegerList
from meresco.oai import OaiJazz

mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertOaiV1ToV2Test(SeecrTestCase):
    def testConversion(self):
        datadir = join(self.tempdir, 'oai_conversion_v1_to_v2')
        copytree(join(mypath, 'data', 'oai_conversion_v1_to_v2'), datadir)

        anotherSet = IntegerList(10, use64bits=True)
        anotherSet.save(join(datadir, 'sets', 'anotherSet.list'), offset=0, append=False)
        anotherSetDeleted = IntegerList(5, use64bits=True)
        anotherSetDeleted.save(join(datadir, 'sets', 'anotherSet.list.deleted'), offset=0, append=False)

        anotherPrefix = IntegerList(10, use64bits=True)
        anotherPrefix.save(join(datadir, 'prefixes', 'anotherPrefix.list'), offset=0, append=False)

        system("%s %s > %s 2>&1" % (
                join(binDir, 'convert_oai_v1_to_v2'),
                join(self.tempdir, 'oai_conversion_v1_to_v2'),
                join(self.tempdir, 'oai_conversion_v1_to_v2.log'),
            ))

        expectedList = PersistentSortedIntegerList(join(self.tempdir, 'forAssertEquals'), use64bits=True)
        for i in xrange(10 ** 3):
            expectedList.append(i)
        expectedList.remove(200)
        expectedList.remove(600)
        expectedList.remove(4)
        for listName in ['tombStones', 'prefixes/somePrefix', 'sets/someSet']:
            converted = PersistentSortedIntegerList(join(datadir, listName + '.list'), use64bits=True)
            self.assertEquals(list(expectedList), list(converted))

        convertedAnotherSet = PersistentSortedIntegerList(join(datadir, 'sets', 'anotherSet.list'), use64bits=True)
        self.assertEquals(range(5, 10), list(convertedAnotherSet))

        convertedAnotherPrefix = PersistentSortedIntegerList(join(datadir, 'prefixes', 'anotherPrefix.list'), use64bits=True)
        self.assertEquals(range(0, 10), list(convertedAnotherPrefix))

        self.assertEquals('2', open(join(datadir, 'oai.version')).read())

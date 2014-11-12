## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
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
from meresco.oai import OaiJazz

mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertOaiV7ToV8Test(SeecrTestCase):
    def testConversion(self):
        datadir = join(self.tempdir, 'oai_conversion_v7_to_v8')
        copytree(join(mypath, 'data', 'oai_conversion_v7_to_v8'), datadir)
        system("%s %s > %s 2>&1" % (
                join(binDir, 'convert_oai_v7_to_v8'),
                datadir,
                join(self.tempdir, 'oai_conversion_v7_to_v8.log'),
            ))
        print open(join(self.tempdir, 'oai_conversion_v7_to_v8.log')).read()
        self.assertEquals('8', open(join(datadir, 'oai.version')).read())
        jazz = OaiJazz(datadir)
        self.assertEquals({'total':5, 'deletes':1}, jazz.getNrOfRecords())
        result = jazz.oaiSelect(prefix='oai_dc', shouldCountHits=True)
        records = list(result.records)
        self.assertEquals(['oai:1', 'oai:3', 'oai:5', 'oai:4', 'oai:2'], [r.identifier for r in records])
        self.assertEquals([False, False, False, False, True], [r.isDeleted for r in records])

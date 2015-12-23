## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
from meresco.oai.oaijazz import PartHash

mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertOaiV8ToV9Test(SeecrTestCase):
    def testConversion(self):
        datadir = join(self.tempdir, 'oai_conversion_v8_to_v9')
        copytree(join(mypath, 'data', 'oai_conversion_v8_to_v9'), datadir)
        system("%s %s --i-know-what-i-am-doing > %s 2>&1" % (
                join(binDir, 'convert_oai_v8_to_v9'),
                datadir,
                join(self.tempdir, 'oai_conversion_v8_to_v9.log'),
            ))
        self.assertEquals('9', open(join(datadir, 'oai.version')).read())
        jazz = OaiJazz(datadir)
        result = jazz.oaiSelect(prefix='oai_dc', shouldCountHits=True, parthash=PartHash.create("1/2"))
        records = list(result.records)
        self.assertEquals(['oai:1', 'oai:5', 'oai:2'], [r.identifier for r in records])
        self.assertEquals([False, False, True], [r.isDeleted for r in records])

        result = jazz.oaiSelect(prefix='oai_dc', shouldCountHits=True, parthash=PartHash.create("2/2"))
        records = list(result.records)
        self.assertEquals(['oai:3', 'oai:4'], [r.identifier for r in records])
        self.assertEquals([False, False], [r.isDeleted for r in records])

        self.assertEquals({'total':5, 'deletes':1}, jazz.getNrOfRecords())

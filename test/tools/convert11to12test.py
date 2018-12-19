## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2018 Seecr (Seek You Too B.V.) https://seecr.nl
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

from meresco.oai.tools.convert11to12 import Convert11to12
from meresco.oai import OaiJazz

from os import listdir, makedirs
from os.path import abspath, dirname, join
from shutil import copytree

mydir = dirname(abspath(__file__))
version11dir = join(dirname(mydir), 'data', 'version_11')

class Convert11to12Test(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.oaiDir = join(self.tempdir, 'oai-to-be-converted')
        copytree(version11dir, self.oaiDir)

    def testPreflightCheck(self):
        Convert11to12(self.oaiDir)._preflightCheck()
        wrongversion = join(self.tempdir, 'wrongversion')
        makedirs(wrongversion)
        with open(join(wrongversion, 'oai.version'), 'w') as f:
            f.write('3')
        self.assertRaises(ValueError, lambda: Convert11to12(wrongversion)._preflightCheck())

    def testPreCondition(self):
        with open(join(self.oaiDir, 'oai.version'), 'w') as f:
            f.write('12')
        o = OaiJazz(self.oaiDir)
        rec = o.getRecord('id:1')
        self.assertTrue(rec.isDeleted)
        self.assertEqual(set(), rec.deletedPrefixes)
        self.assertEqual({'A', 'B'}, rec.prefixes)

    def testConvert(self):
        Convert11to12(self.oaiDir).go()
        o = OaiJazz(self.oaiDir)
        rec = o.getRecord('id:1')
        self.assertTrue(rec.isDeleted)
        self.assertEqual({'A', 'B'}, rec.deletedPrefixes)
        self.assertEqual({'A', 'B'}, rec.prefixes)

## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2012 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Kennisnet http://www.kennisnet.nl
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

from seecr.test import CallTrace
from unittest import TestCase

from weightless.core import compose
from meresco.oai import OaiAddRecordWithDefaults


class OaiAddRecordWithDefaultsTest(TestCase):
    def testAdd(self):
        subject = OaiAddRecordWithDefaults(sets="SETS", metadataFormats="METADATAFORMATS")
        observer = CallTrace('oaijazz')
        subject.addObserver(observer)

        list(compose(subject.add('id', ignored="kwarg")))

        self.assertEquals(['addOaiRecord'], [m.name for m in observer.calledMethods])
        self.assertEquals({'identifier':'id',
            'sets':'SETS',
            'metadataFormats': 'METADATAFORMATS'},
            observer.calledMethods[0].kwargs)


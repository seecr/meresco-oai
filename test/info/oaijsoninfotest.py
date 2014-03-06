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
from meresco.oai.info import OaiJsonInfo
from meresco.oai import OaiJazz
from weightless.core import asString, consume
from simplejson import loads
from meresco.core import Observable

class OaiJsonInfoTest(SeecrTestCase):

    def setUp(self):
        super(OaiJsonInfoTest, self).setUp()
        self.observable = Observable()
        self.oaiJsonInfo = OaiJsonInfo()
        self.observable.addObserver(self.oaiJsonInfo)
        self.jazz = OaiJazz(self.tempdir)
        self.oaiJsonInfo.addObserver(self.jazz)
        self.jazz.addOaiRecord(identifier='record1', sets=[('set1', 'set1')], metadataFormats=[('prefix1', '', '')])
        self.jazz.addOaiRecord(identifier='record2', sets=[('set1', 'set1')], metadataFormats=[('prefix1', '', ''), ('oai', 'oai-schema', 'oai-namespace')])
        self.jazz.addOaiRecord(identifier='record3', sets=[('set1', 'set1'), ('set2', 'set name 2')], metadataFormats=[('prefix1', '', '')])
        consume(self.jazz.delete(identifier='record3'))
        self.jazz.commit()

    def testGetAllSets(self):
        result = asString(self.observable.all.handleRequest(path='/info/json/sets', arguments={}))
        header, body = result.split('\r\n\r\n')
        self.assertEquals(['set1', 'set2'], loads(body))

    def testGetAllPrefixes(self):
        result = asString(self.observable.all.handleRequest(path='/info/json/prefixes', arguments={}))
        header, body = result.split('\r\n\r\n')
        self.assertEquals(['oai', 'prefix1'], loads(body))

    def testPrefixInfo(self):
        result = asString(self.observable.all.handleRequest(path='/info/json/prefix', arguments=dict(prefix=['prefix1'])))
        header, body = result.split('\r\n\r\n')
        self.assertEquals(dict(prefix='prefix1', schema='', namespace='', nrOfRecords=3), loads(body))

        result = asString(self.observable.all.handleRequest(path='/info/json/prefix',
            arguments=dict(prefix=['oai'])))
        header, body = result.split('\r\n\r\n')
        self.assertEquals(dict(prefix='oai', schema='oai-schema', namespace='oai-namespace', nrOfRecords=1), loads(body))

    def testUnknownPrefixInfo(self):
        result = asString(self.observable.all.handleRequest(path='/info/json/prefix',
            arguments=dict(prefix=['unknown'])))
        header, body = result.split('\r\n\r\n')
        self.assertEquals({}, loads(body))

    def testSetInfo(self):
        result = asString(self.observable.all.handleRequest(path='/info/json/set', arguments=dict(set=['set1'])))
        header, body = result.split('\r\n\r\n')
        self.assertEquals(dict(setSpec='set1', name='set1', nrOfRecords=3), loads(body))

        result = asString(self.observable.all.handleRequest(path='/info/json/set',
            arguments=dict(set=['set2'])))
        header, body = result.split('\r\n\r\n')
        self.assertEquals(dict(setSpec='set2', name='set name 2', nrOfRecords=1), loads(body))

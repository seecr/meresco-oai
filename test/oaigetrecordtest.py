## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from StringIO import StringIO
from lxml.etree import parse, XML

from seecr.test import SeecrTestCase, CallTrace

from weightless.core import asString, consume
from meresco.xml.namespaces import xpath

from meresco.sequentialstore import MultiSequentialStorage

from meresco.oai import OaiJazz
from meresco.oai.oaigetrecord import OaiGetRecord
from meresco.oai.oairecord import OaiRecord
from meresco.oai.oairepository import OaiRepository


class OaiGetRecordTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server'},
            'port':9000,
        }

    def testGetRecordWithMultiSequentialStorage(self):
        oaigetrecord = OaiGetRecord()
        oaijazz = OaiJazz(self.tempdir + '/jazz')
        storage = MultiSequentialStorage(self.tempdir + "/seq-store")
        oairecord = OaiRecord()
        oairecord.addObserver(storage)
        oaigetrecord.addObserver(oaijazz)
        oaigetrecord.addObserver(oairecord)
        oaijazz.addOaiRecord(identifier="id0", sets=(), metadataFormats=[('oai_dc', '', '')])
        storage.addData(identifier="id0", name="oai_dc", data="data01")
        response = oaigetrecord.getRecord(arguments=dict(
                verb=['GetRecord'],
                metadataPrefix=['oai_dc'],
                identifier=['id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals("data01", xpath(parse(StringIO(body)), '//oai:metadata')[0].text)

    def testGetRecordWithRepositoryIdentifier(self):
        oaigetrecord = OaiGetRecord(OaiRepository(identifier='example.org'))
        record = CallTrace('record')
        record.identifier = 'id0'
        record.prefixes = ['oai_dc']
        record.sets = []
        record.isDeleted = False
        observer = CallTrace(returnValues={
            'getAllPrefixes': ['oai_dc'],
            'getRecord': record},
            emptyGeneratorMethods=['oaiWatermark', 'oaiRecord'])
        oaigetrecord.addObserver(observer)
        consume(oaigetrecord.getRecord(arguments=dict(
                verb=['GetRecord'],
                metadataPrefix=['oai_dc'],
                identifier=['oai:example.org:id0'],
            ),
            **self.httpkwargs))
        self.assertEquals(['getRecord', 'getAllPrefixes', 'oaiWatermark', 'oaiRecord'], observer.calledMethodNames())
        self.assertEquals(dict(identifier='id0'), observer.calledMethods[0].kwargs)

    def testGetRecordWithRepositoryIdentifierMissingExpectedPrefix(self):
        oaigetrecord = OaiGetRecord(OaiRepository(identifier='example.org'))
        result = asString(oaigetrecord.getRecord(arguments=dict(
            verb=['GetRecord'],
            metadataPrefix=['oai_dc'],
            identifier=['not:properly:prefixed:id0'],
        ),
        **self.httpkwargs))
        header, body = result.split('\r\n\r\n')
        self.assertTrue(xpath(XML(body), '/oai:OAI-PMH/oai:error[@code="idDoesNotExist"]'), body)

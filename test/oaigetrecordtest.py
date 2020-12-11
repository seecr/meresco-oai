## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014-2016, 2018 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 SURFmarket https://surf.nl
# Copyright (C) 2018 Stichting Kennisnet https://www.kennisnet.nl
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

from io import BytesIO
from lxml.etree import parse, XML

from seecr.test import SeecrTestCase, CallTrace

from weightless.core import asString, consume, be
from meresco.xml.namespaces import xpath, xpathFirst

from meresco.components import RetrieveToGetDataAdapter
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
        oaijazz = OaiJazz(self.tempdir + '/jazz')
        storage = MultiSequentialStorage(self.tempdir + "/seq-store")
        oairecord = OaiRecord()
        oaigetrecord = be((OaiGetRecord(repository=OaiRepository()),
            (oaijazz,),
            (oairecord,
                (RetrieveToGetDataAdapter(),
                    (storage,)
                )
            )
        ))

        oaijazz.addOaiRecord(identifier="id0", sets=(), metadataFormats=[('oai_dc', '', '')])
        storage.addData(identifier="id0", name="oai_dc", data="data01")
        response = oaigetrecord.getRecord(arguments=dict(
                verb=['GetRecord'],
                metadataPrefix=['oai_dc'],
                identifier=['id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEqual("data01", xpath(parse(BytesIO(body.encode())), '//oai:metadata')[0].text)

    def testGetRecordDeletedInRequestedPrefix(self):
        oaijazz = OaiJazz(self.tempdir + '/jazz')
        storage = MultiSequentialStorage(self.tempdir + "/seq-store")
        oairecord = OaiRecord()
        class MyStorage(object):
            def getData(self, identifier, name):
                return 'data'
        oaigetrecord = be((OaiGetRecord(repository=OaiRepository()),
            (oaijazz,),
            (oairecord,
                (MyStorage(),)
            )
        ))
        oaijazz.addOaiRecord(identifier='id:0', metadataPrefixes=['A', 'B'])
        oaijazz.deleteOaiRecordInPrefixes(identifier='id:0', metadataPrefixes=['A'])
        response = oaigetrecord.getRecord(arguments=dict(
                verb=['GetRecord'],
                metadataPrefix=['A'],
                identifier=['id:0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEqual('deleted', xpathFirst(XML(body.encode()), '/oai:OAI-PMH/oai:GetRecord/oai:record/oai:header/@status'), body)

        response = oaigetrecord.getRecord(arguments=dict(
                verb=['GetRecord'],
                metadataPrefix=['B'],
                identifier=['id:0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEqual("data", xpathFirst(XML(body.encode()), '//oai:metadata/text()'))

        response = oaigetrecord.getRecord(arguments=dict(
                verb=['GetRecord'],
                metadataPrefix=['C'],
                identifier=['id:0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEqual('cannotDisseminateFormat', xpathFirst(XML(body.encode()), '/oai:OAI-PMH/oai:error/@code'))

    def testGetRecordWithRepositoryIdentifier(self):
        oaigetrecord = OaiGetRecord(OaiRepository(identifier='example.org'))
        record = CallTrace('record')
        record.identifier = 'id0'
        record.prefixes = ['oai_dc']
        record.sets = []
        record.isDeleted = False
        observer = CallTrace(returnValues={
            'isKnownPrefix': True,
            'getRecord': record},
            emptyGeneratorMethods=['oaiWatermark', 'oaiRecord'])
        oaigetrecord.addObserver(observer)
        consume(oaigetrecord.getRecord(arguments=dict(
                verb=['GetRecord'],
                metadataPrefix=['oai_dc'],
                identifier=['oai:example.org:id0'],
            ),
            **self.httpkwargs))
        self.assertEqual(['getRecord', 'isKnownPrefix', 'oaiWatermark', 'oaiRecord'], observer.calledMethodNames())
        self.assertEqual(dict(identifier='id0', metadataPrefix='oai_dc'), observer.calledMethods[0].kwargs)

    def testGetRecordWithRepositoryIdentifierMissingExpectedPrefix(self):
        oaigetrecord = OaiGetRecord(OaiRepository(identifier='example.org'))
        result = asString(oaigetrecord.getRecord(arguments=dict(
            verb=['GetRecord'],
            metadataPrefix=['oai_dc'],
            identifier=['not:properly:prefixed:id0'],
        ),
        **self.httpkwargs))
        header, body = result.split('\r\n\r\n')
        self.assertEqual('idDoesNotExist', xpathFirst(XML(body.encode()), '/oai:OAI-PMH/oai:error/@code'))

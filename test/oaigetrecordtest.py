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
from meresco.sequentialstore import MultiSequentialStorage
from meresco.oai import OaiJazz
from meresco.oai.oaigetrecord import OaiGetRecord
from meresco.oai.oairecord import OaiRecord
from uuid import uuid4
from weightless.core import asString
from lxml.etree import parse
from StringIO import StringIO
from meresco.xml.namespaces import xpath


class OaiGetRecordTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.clientId = str(uuid4())
        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server', 'X-Meresco-Oai-Client-Identifier': self.clientId},
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

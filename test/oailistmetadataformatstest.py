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

from lxml.etree import XML

from seecr.test import SeecrTestCase

from weightless.core import asString
from meresco.xml import xpath

from meresco.oai.oailistmetadataformats import OaiListMetadataFormats
from meresco.oai.oaijazz import OaiJazz
from meresco.oai.oairepository import OaiRepository


class OaiListMetadataFormatsTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server'},
            'port':9000,
        }

    def testListMetadataFormats(self):
        listMetadataFormats = OaiListMetadataFormats()
        oaijazz = OaiJazz(self.tempdir + '/jazz')
        listMetadataFormats.addObserver(oaijazz)
        oaijazz.addOaiRecord(identifier="id0", sets=(), metadataFormats=[('oai_dc', '', '')])
        oaijazz.addOaiRecord(identifier="id1", sets=(), metadataFormats=[('rdf', '', '')])
        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals(['oai_dc', 'rdf'], xpath(XML(body), '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat/oai:metadataPrefix/text()'))
        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
                identifier=['id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals(['oai_dc'], xpath(XML(body), '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat/oai:metadataPrefix/text()'))

    def testListMetadataFormatsWithRepositoryIdentifier(self):
        listMetadataFormats = OaiListMetadataFormats(repository=OaiRepository('example.org'))
        oaijazz = OaiJazz(self.tempdir + '/jazz')
        listMetadataFormats.addObserver(oaijazz)
        oaijazz.addOaiRecord(identifier="id0", sets=(), metadataFormats=[('oai_dc', '', '')])
        oaijazz.addOaiRecord(identifier="id1", sets=(), metadataFormats=[('rdf', '', '')])
        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
                identifier=['id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertTrue(xpath(XML(body), '/oai:OAI-PMH/oai:error[@code="idDoesNotExist"]'), body)

        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
                identifier=['oai:example.org:id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals(['oai_dc'], xpath(XML(body), '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat/oai:metadataPrefix/text()'))

## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014, 2019-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from meresco.oai.oaiutils import OaiException
from meresco.oai.oairepository import OaiRepository


class OaiRepositoryTest(SeecrTestCase):
    def testBasicProperties(self):
        oaiRepository = OaiRepository(identifier='example.org', name='The Repository Name', adminEmail='admin@meresco.org')
        self.assertEqual('example.org', oaiRepository.identifier)
        self.assertEqual('The Repository Name', oaiRepository.name)
        self.assertEqual('admin@meresco.org', oaiRepository.adminEmail)

    def testBadRepositoryIdentifier(self):
        def oaiRepository(repositoryIdentifier):
            return OaiRepository(identifier=repositoryIdentifier, name='The Repository Name', adminEmail='admin@meresco.org')
        self.assertRaises(ValueError, lambda: oaiRepository('01234'))
        self.assertRaises(ValueError, lambda: oaiRepository('a*'))
        self.assertRaises(ValueError, lambda: oaiRepository('a34.0834'))

    def testPrefixIdentifier(self):
        oaiRepository = OaiRepository(name='The Repository Name')
        self.assertEqual("id0", oaiRepository.prefixIdentifier('id0'))

        oaiRepository = OaiRepository(identifier='example.org')
        self.assertEqual("oai:example.org:id0", oaiRepository.prefixIdentifier('id0'))

    def testUnprefixIdentifier(self):
        oaiRepository = OaiRepository(name='The Repository Name')
        self.assertEqual("id0", oaiRepository.unprefixIdentifier('id0'))
        self.assertEqual("oai:example.org:id0", oaiRepository.unprefixIdentifier('oai:example.org:id0'))

        oaiRepository = OaiRepository(identifier='example.org')
        self.assertEqual("id0", oaiRepository.unprefixIdentifier("oai:example.org:id0"))

        try:
            self.assertEqual("id0", oaiRepository.unprefixIdentifier("some:other:prefix:id0"))
            self.fail()
        except OaiException as e:
            self.assertEqual('', str(e))

    def testRequestUrl(self):
        oaiRepository = OaiRepository(identifier='example.org')
        self.assertEqual("http://hostname:8080/path", oaiRepository.requestUrl(Headers={'Host': 'hostname:9090'}, path='/path', port=8080))
        self.assertEqual("http://hostname:8080/path", oaiRepository.requestUrl(Headers={'Host': 'hostname:8080'}, path='/path', port=8080))
        self.assertEqual("http://hostname/path", oaiRepository.requestUrl(Headers={'Host': 'hostname:8080'}, path='/path', port=80))

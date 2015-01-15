## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase, CallTrace
from meresco.oai.tools.iterateoaipmh import OaiListRequest

class IterateOaiPmhTest(SeecrTestCase):
    def setUp(self):
        super(IterateOaiPmhTest, self).setUp()
        self.opener = CallTrace('urlopen')
        self.urlopen = self.opener.urlopen

    def testBuildUrl(self):
        def request(**kwargs):
            return OaiListRequest(baseurl='http://example.org/oai', verb='ListRecords', **kwargs)
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai_dc', request(metadataPrefix='oai_dc').buildUrl())
        self.assertEquals('http://example.org/oai?verb=ListRecords&from=2014&metadataPrefix=oai_dc', request(metadataPrefix='oai_dc', from_='2014').buildUrl())
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai_dc&set=someset', request(metadataPrefix='oai_dc', set='someset').buildUrl())
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai_dc&until=2016', request(metadataPrefix='oai_dc', until='2016').buildUrl())

    def testNewWithResumptionToken(self):
        request = OaiListRequest(baseurl='http://example.org/oai', verb='ListRecords', metadataPrefix='oai', set='set')
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai&set=set', request.buildUrl())
        request = request.newWithResumptionToken('resume_here')
        self.assertEquals('http://example.org/oai?verb=ListRecords&resumptionToken=resume_here', request.buildUrl())

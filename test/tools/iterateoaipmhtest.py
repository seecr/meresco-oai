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

from StringIO import StringIO
from lxml.etree import XML

from seecr.test import SeecrTestCase, CallTrace

from meresco.oai.tools.iterateoaipmh import OaiListRequest, OaiBatch


class IterateOaiPmhTest(SeecrTestCase):
    def testBuildUrl(self):
        def request(**kwargs):
            return OaiListRequest(baseurl='http://example.org/oai', verb='ListRecords', **kwargs)
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai_dc', request(metadataPrefix='oai_dc').buildUrl())
        self.assertEquals('http://example.org/oai?verb=ListRecords&from=2014&metadataPrefix=oai_dc', request(metadataPrefix='oai_dc', from_='2014').buildUrl())
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai_dc&set=someset', request(metadataPrefix='oai_dc', set='someset').buildUrl())
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai_dc&until=2016', request(metadataPrefix='oai_dc', until='2016').buildUrl())

    def testNextRequest(self):
        request = OaiListRequest(baseurl='http://example.org/oai', verb='ListRecords', metadataPrefix='oai', set='set')
        self.assertEquals('http://example.org/oai?verb=ListRecords&metadataPrefix=oai&set=set', request.buildUrl())
        request = request._nextRequest('resume_here')
        self.assertEquals('http://example.org/oai?verb=ListRecords&resumptionToken=resume_here', request.buildUrl())

    def testBatch(self):
        request = OaiListRequest(baseurl='ignored', verb='ListRecords', metadataPrefix='oai_dc')
        batch = OaiBatch(request=request, response=XML(RESPONSE))
        self.assertEquals('u|c1417616627182914|mese|s|f', batch.resumptionToken)
        self.assertEquals('2015-03-05T08:54:37Z', batch.responseDate)
        items = batch.items
        self.assertEquals(3, len(items))
        item1, item2, item3 = items
        self.assertEquals('oai:id:123', item1.identifier)
        self.assertEquals('2014-12-03T14:23:24Z', item1.datestamp)
        self.assertEquals(False, item1.deleted)
        self.assertEquals(['set1', 'set1:subset1', 'set2'], item1.setSpecs)
        self.assertXmlEquals("""<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <dc:title xml:lang="en">Some Title</dc:title>
            </oai_dc:dc>""", item1.metadata)
        self.assertEquals(batch, item1.oaiBatch)
        self.assertEquals('oai:id:124', item2.identifier)
        self.assertEquals(True, item2.deleted)
        self.assertEquals('oai:id:125', item3.identifier)
        self.assertEquals(False, item3.deleted)
        self.assertXmlEquals("""<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <dc:description>test</dc:description>
            </oai_dc:dc>""", item3.metadata)

    def testRetrieveBatch(self):
        request = OaiListRequest(baseurl='http://example.org/oai', verb='ListRecords', metadataPrefix='oai_dc')
        opener = CallTrace('urlopen', returnValues={'urlopen': StringIO(RESPONSE)})
        request._urlopen = opener.urlopen
        batch = request.retrieveBatch()
        self.assertEquals(3, len(batch.items))
        self.assertEquals(['urlopen'], opener.calledMethodNames())
        urlopenMethod = opener.calledMethods[0]
        self.assertEquals((('http://example.org/oai?verb=ListRecords&metadataPrefix=oai_dc',), {}), (urlopenMethod.args, urlopenMethod.kwargs))

    def testNoRecordsMatch(self):
        request = OaiListRequest(baseurl='http://example.org/oai', verb='ListRecords', metadataPrefix='oai_dc')
        opener = CallTrace('urlopen', returnValues={'urlopen': StringIO(NO_RECORDS_MATCH_RESPONSE)})
        request._urlopen = opener.urlopen
        batch = request.retrieveBatch()
        self.assertEquals(0, len(batch.items))


NO_RECORDS_MATCH_RESPONSE = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"><responseDate>2015-10-08T10:08:50Z</responseDate><request metadataPrefix="oai_dc" verb="ListRecords">http://example.org/oai</request><error code="noRecordsMatch">The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list.</error></OAI-PMH>"""

RESPONSE = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
<responseDate>2015-03-05T08:54:37Z</responseDate><request metadataPrefix="oai_dc" verb="ListRecords">http://example.org/oai</request><ListRecords>
    <record>
        <header>
            <identifier>oai:id:123</identifier>
            <datestamp>2014-12-03T14:23:24Z</datestamp>
            <setSpec>set1</setSpec>
            <setSpec>set1:subset1</setSpec>
            <setSpec>set2</setSpec>
        </header>
        <metadata>
            <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
                <dc:title xml:lang="en">Some Title</dc:title>
            </oai_dc:dc>
        </metadata>
    </record>
    <record>
        <header status="deleted">
            <identifier>oai:id:124</identifier>
            <datestamp>2014-12-03T14:23:25Z</datestamp>
        </header>
    </record>
    <record>
        <header>
            <identifier>oai:id:125</identifier>
            <datestamp>2014-12-03T14:23:26Z</datestamp>
            <setSpec>set3</setSpec>
        </header>
        <metadata>
            <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
                <dc:description>test</dc:description>
            </oai_dc:dc>
        </metadata>
    </record>
    <resumptionToken>u|c1417616627182914|mese|s|f</resumptionToken>
</ListRecords>
</OAI-PMH>"""
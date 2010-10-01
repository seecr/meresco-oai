## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
#    Copyright (C) 2007-2009 Stichting Kennisnet Ict op school.
#       http://www.kennisnetictopschool.nl
#    Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
#    Copyright (C) 2009 Tilburg University http://www.uvt.nl
#    Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
#
#    This file is part of Meresco Oai.
#
#    Meresco Oai is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    Meresco Oai is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Meresco Oai; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##
from lxml.etree import parse, tostring
from StringIO import StringIO

from amara.binderytools import bind_string
from cq2utils.calltrace import CallTrace
from itertools import imap

from mockoaijazz import MockOaiJazz

from meresco.components.http.utils import CRLF
from meresco.core import ObserverFunction
from meresco.oai.oailist import BATCH_SIZE, OaiList
from meresco.oai.resumptiontoken import resumptionTokenFromString, ResumptionToken

from oaitestcase import OaiTestCase
from meresco.oai.oaiutils import OaiException

from weightless import compose

class OaiListTest(OaiTestCase):
    def getSubject(self):
        oailist = OaiList()
        oailist.addObserver(ObserverFunction(lambda: ['oai_dc'], 'getAllPrefixes'))
        return oailist

    def testListRecordsUsingMetadataPrefix(self):
        self.request.args = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}

        mockoaijazz = MockOaiJazz(
            selectAnswer=['id_0&0', 'id_1&1'],
            selectTotal=2,
            isAvailableDefault=(True,True),
            isAvailableAnswer=[
                (None, 'oai_dc', (True,False)),
                (None, '__tombstone__', (True, False))])
        self.subject.addObserver(mockoaijazz)
        
        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]
        self.assertTrue("""<identifier>id_0&amp;0</identifier>""" in body, body)
        self.assertTrue("""<identifier>id_1&amp;1</identifier>""" in body, body)
        self.assertTrue("""<some:recorddata xmlns:some="http://some.example.org" id="id_0&amp;0"/>""" in body, body)
        self.assertTrue("""<some:recorddata xmlns:some="http://some.example.org" id="id_1&amp;1"/>""" in body, body)
        self.assertTrue(body.find('<resumptionToken') == -1)
        self.assertFalse(mockoaijazz.oaiSelectArguments[0])

    def testListRecordsWithoutProvenance(self):
        self.request.args = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}

        self.subject.addObserver(MockOaiJazz(
            selectAnswer=['id_0&0', 'id_1&1'],
            isAvailableDefault=(True,True),
            isAvailableAnswer=[
                (None, 'oai_dc', (True,False)),
                (None, '__tombstone__', (True, False))]))

        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]
        self.assertFalse('<about' in body)

    def testListRecordsWithProvenance(self):
        class MockOaiProvenance(object):
            def provenance(inner, id):
                yield "PROVENANCE"

        self.request.args = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}

        self.subject.addObserver(MockOaiProvenance())
        self.subject.addObserver(MockOaiJazz(
            selectAnswer=['id_0&0', 'id_1&1'],
            isAvailableDefault=(True,True),
            isAvailableAnswer=[
                (None, 'oai_dc', (True,False)),
                (None, '__tombstone__', (True, False))]))

        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]
        self.assertTrue('<about>PROVENANCE</about>' in body, body)

    def testListRecordsUsingToken(self):
        self.request.args = {'verb':['ListRecords'], 'resumptionToken': [str(ResumptionToken('oai_dc', '10', 'FROM', 'UNTIL', 'SET'))]}

        observer = CallTrace('RecordAnswering')
        def oaiSelect(sets, prefix, continueAfter, oaiFrom, oaiUntil):
            self.assertEquals('SET', sets[0])
            self.assertEquals('oai_dc', prefix)
            self.assertEquals('10', continueAfter)
            self.assertEquals('FROM', oaiFrom)
            self.assertEquals('UNTIL', oaiUntil)
            return (f for f in [])

        observer.oaiSelect = oaiSelect
        self.subject.addObserver(observer)
        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))

    def testResumptionTokensAreProduced(self):
        self.request.args = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'from': ['2000-01-01T00:00:00Z'], 'until': ['2000-12-31T00:00:00Z'], 'set': ['SET']}
        observer = CallTrace('RecordAnswering')
        def oaiSelect(sets, prefix, continueAfter, oaiFrom, oaiUntil):
            return imap(lambda i: 'id_%i' % i, xrange(999999))
        def writeRecord(*args, **kwargs):
            pass
        def provenance(*args, **kwargs):
            yield ""
        def yieldRecord(*args, **kwargs):
            yield ""
        observer.oaiSelect = oaiSelect
        observer.provenance = provenance
        observer.yieldRecord = yieldRecord
        observer.getUnique = lambda x: 'UNIQUE_FOR_TEST'
        self.subject.addObserver(observer)
        self.subject.writeRecord = writeRecord
        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]
        self.assertTrue(body.find("<resumptionToken>") > -1)
        xml = bind_string(body).OAI_PMH.ListRecords.resumptionToken
        resumptionToken = resumptionTokenFromString(str(xml))
        self.assertEquals('UNIQUE_FOR_TEST', resumptionToken._continueAfter)
        self.assertEquals('oai_dc', resumptionToken._metadataPrefix)
        self.assertEquals('2000-01-01T00:00:00Z', resumptionToken._from)
        self.assertEquals('2000-12-31T00:00:00Z', resumptionToken._until)
        self.assertEquals('SET', resumptionToken._set)

    def testFinalResumptionToken(self):
        self.request.args = {'verb':['ListRecords'], 'resumptionToken': [str(ResumptionToken('oai_dc', '200'))]}

        self.subject.addObserver(MockOaiJazz(selectAnswer=map(lambda i: 'id_%i' % i, range(BATCH_SIZE)), selectTotal = BATCH_SIZE))
        self.subject.writeRecord = lambda *args, **kwargs: None

        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]

        self.assertTrue(body.find("<resumptionToken") > -1)
        self.assertEquals('', str(bind_string(body).OAI_PMH.ListRecords.resumptionToken))

    def testDeletedTombstones(self):
        self.request.args = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}

        self.subject.addObserver(MockOaiJazz(
            selectAnswer=['id_0', 'id_1'],
            deleted=['id_1'],
            isAvailableDefault=(True,False),
            selectTotal = 2))

        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]
        self.assertTrue("""<header>
            <identifier>id_0</identifier>""" in body, body)
        self.assertTrue("""<header status="deleted">
            <identifier>id_1</identifier>""" in body, body)

        self.assertTrue(self.stream.getvalue().find('<resumptionToken') == -1)

    def testFromAndUntil(self):
        #ok, deze test wordt zo lang dat het haast wel lijkt of hier iets niet klopt.... KVS

        observer = MockOaiJazz(
            selectAnswer=['id_0', 'id_1'],
            isAvailableDefault=(True, False),
            isAvailableAnswer=[("id_1", "__tombstone__", (True, True))])

        self.subject.addObserver(observer)

        def doIt(oaiFrom, oaiUntil):
            self.request.args = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}
            if oaiFrom:
                self.request.args['from'] = [oaiFrom]
            if oaiUntil:
                self.request.args['until'] = [oaiUntil]
            result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
            self.body = result.split(CRLF*2)[-1]
            return [observer.oaiSelectArguments[3], observer.oaiSelectArguments[4]]

        def right(oaiFrom, oaiUntil, expectedFrom = None, expectedUntil = None):
            expectedFrom = expectedFrom or oaiFrom
            expectedUntil = expectedUntil or oaiUntil
            resultingOaiFrom, resultingOaiUntil = doIt(oaiFrom, oaiUntil)
            self.assertEquals(expectedFrom, resultingOaiFrom)
            self.assertEquals(expectedUntil, resultingOaiUntil)
            self.assertTrue(not "<error" in self.body, self.body)

        def wrong(oaiFrom, oaiUntil):
            doIt(oaiFrom, oaiUntil)
            self.assertTrue("""<error code="badArgument">""" in self.body)

        #start reading here
        right(None, None)
        right('2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z')
        right('2000-01-01', '2000-01-01', '2000-01-01T00:00:00Z', '2000-01-01T23:59:59Z')
        right(None, '2000-01-01T00:00:00Z')
        right('2000-01-01T00:00:00Z', None)
        wrong('thisIsNotEvenADateStamp', 'thisIsNotEvenADateStamp')
        wrong('2000-01-01T00:00:00Z', '2000-01-01')
        wrong('2000-01-01T00:00:00Z', '1999-01-01T00:00:00Z')

    def testListIdentifiers(self):
        self.request.args = {'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}

        self.subject.addObserver(MockOaiJazz(
            selectAnswer=['id_0'],
            isAvailableDefault=(True,False),
            isAvailableAnswer=[(None, 'oai_dc', (True,True))],
            selectTotal=1))
        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]

        self.assertTrue("""<request metadataPrefix="oai_dc"
 verb="ListIdentifiers">http://server:9000/path/to/oai</request>
 <ListIdentifiers>
    <header>
      <identifier>id_0</identifier>
      <datestamp>DATESTAMP_FOR_TEST</datestamp>
    </header>
 </ListIdentifiers>""", body)

    def testListIdentifiersWithProvenance(self):
        class MockOaiProvenance(object):
            def provenance(inner, id):
                yield "PROVENANCE"
        self.request.args = {'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}

        self.subject.addObserver(MockOaiJazz(
            selectAnswer=['id_0'],
            isAvailableDefault=(True,False),
            isAvailableAnswer=[(None, 'oai_dc', (True,True))],
            selectTotal=1))
        self.subject.addObserver(MockOaiProvenance())
        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]
        self.assertFalse('<about>PROVENANCE</about>' in body, body)

    def testNoRecordsMatch(self):
        self.request.args = {'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}

        self.subject.addObserver(MockOaiJazz(selectTotal = 0))
        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        body = result.split(CRLF*2)[-1]
        self.assertTrue(body.find("noRecordsMatch") > -1)

    def testSetsInHeader(self):
        self.request.args = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}

        self.subject.addObserver(MockOaiJazz(
            selectAnswer=['id_0&0', 'id_1&1'],
            setsAnswer=['one:two:three', 'one:two:four'],
            isAvailableDefault=(True,False),
            isAvailableAnswer=[
                (None, 'oai_dc', (True, True)),
                (None, '__sets__', (True, True))]))
        result = ''.join(compose(self.observable.all.listRecords(self.request.args, **self.request.kwargs)))
        
        self.assertTrue("<setSpec>one:two:three</setSpec>" in result)
        self.assertTrue("<setSpec>one:two:four</setSpec>" in result)


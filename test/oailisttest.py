## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from xml.sax.saxutils import escape as escapeXml
from lxml.etree import parse
from uuid import uuid4
from os import makedirs

from seecr.test import SeecrTestCase, CallTrace
from seecr.test.io import stderr_replaced

from weightless.core import compose, Yield, NoneOfTheObserversRespond, asString, consume
from meresco.components.http.utils import CRLF

from meresco.oai.oailist import OaiList
from meresco.oai import OaiJazz, SequentialMultiStorage

from meresco.oai.resumptiontoken import ResumptionToken
from meresco.oai.oairecord import OaiRecord


class OaiListTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.oaiJazz = OaiJazz(self.tempdir)
        self.oaiList = OaiList(batchSize=2)
        self.observer = CallTrace('observer')
        self.observer.methods['suspend'] = lambda clientIdentifier: (s for s in ['SUSPEND'])
        self.observer.methods['oaiWatermark'] = lambda o=None: (x for x in ["Crafted By Seecr"])
        def oaiRecord(record, metadataPrefix, data=None):
            yield '<mock:record xmlns:mock="uri:mock">%s/%s</mock:record>' % (escapeXml(record.identifier), escapeXml(metadataPrefix))
        self.observer.methods['oaiRecord'] = oaiRecord
        self.observer.methods['oaiRecordHeader'] = oaiRecord
        self.observer.methods['getAllPrefixes'] = self.oaiJazz.getAllPrefixes
        self.observer.methods['oaiSelect'] = self.oaiJazz.oaiSelect
        def iterData(**kwargs):
            raise NoneOfTheObserversRespond('No one', 0)
        self.observer.methods['iterData'] = iterData
        self.oaiList.addObserver(self.observer)
        self.clientId = str(uuid4())
        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server', 'X-Meresco-Oai-Client-Identifier': self.clientId},
            'port':9000,
        }

    def testListRecords(self):
        self._addRecords(['id:0&0', 'id:1&1'])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEquals(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'iterData', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=None, batchSize=2, shouldCountHits=False), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[4:]
        self.assertEquals({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(recordMethods[1].kwargs))

    def testListRecordsWithSequentialMultiStorage(self):
        oaijazz = OaiJazz(self.tempdir + '/1')
        oailist = OaiList(batchSize=2)
        makedirs(self.tempdir + "/2")
        oaistorage = SequentialMultiStorage(self.tempdir + "/2")
        oailist.addObserver(oaijazz)
        oairecord = OaiRecord()
        oailist.addObserver(oaistorage)
        oailist.addObserver(oairecord)
        stamp = oaijazz.addOaiRecord("id0", (), metadataFormats=[('oai_dc', '', '')])
        consume(oaistorage.add(str(stamp), "oai_dc", "data01"))
        response = oailist.listRecords(arguments=dict(
                verb=['ListRecords'], metadataPrefix=['oai_dc']), **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals("data01", xpath(parse(StringIO(body)), '//oai:metadata')[0].text)


    def testListIdentifiers(self):
        self._addRecords(['id:0&0', 'id:1&1'])

        header, body = ''.join(compose(self.oaiList.listIdentifiers(arguments={'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListIdentifiers/mock:record')))
        self.assertEquals(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'iterData', 'oaiRecordHeader', 'oaiRecordHeader'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=None, batchSize=2, shouldCountHits=False), selectMethod.kwargs)
        headerMethods = self.observer.calledMethods[4:]
        self.assertEquals({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, _m(headerMethods[0].kwargs))
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(headerMethods[1].kwargs))

    def testListRecordsProducesResumptionToken(self):
        self._addRecords(['id:0&0', 'id:1&1', 'id:2&2'], sets=[('set0', 'setName')])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'from': ['2000-01-01T00:00:00Z'], 'until': ['4012-01-01T00:00:00Z'], 'set': ['set0']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumptionToken = ResumptionToken.fromString(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')[0])
        self.assertEquals('4012-01-01T00:00:00Z', resumptionToken.until)
        self.assertEquals('2000-01-01T00:00:00Z', resumptionToken.from_)
        self.assertEquals('set0', resumptionToken.set_)
        self.assertEquals('oai_dc', resumptionToken.metadataPrefix)
        continueAfter = self.oaiJazz.getRecord('id:1&1').stamp
        self.assertEquals(str(continueAfter), resumptionToken.continueAfter)
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'iterData', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals(dict(continueAfter='0', oaiUntil='4012-01-01T00:00:00Z', prefix='oai_dc', oaiFrom='2000-01-01T00:00:00Z', sets=['set0'], batchSize=2, shouldCountHits=False), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[4:]
        self.assertEquals({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(recordMethods[1].kwargs))

    def testListRecordsUsesGivenResumptionToken(self):
        self._addRecords(['id:2&2'], sets=[('set0', 'setName')])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken':['u4012-01-01T00:00:00Z|c1000|moai_dc|sset0|f2000-01-01T00:00:00Z']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'iterData', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals(dict(continueAfter='1000', oaiUntil='4012-01-01T00:00:00Z', prefix='oai_dc', oaiFrom='2000-01-01T00:00:00Z', sets=['set0'], batchSize=2, shouldCountHits=False), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[4:]
        self.assertEquals({'recordId':'id:2&2', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))

    def testListRecordsEmptyFinalResumptionToken(self):
        self._addRecords(['id:2&2', 'id:3&3'])
        resumptionToken = str(ResumptionToken(metadataPrefix='oai_dc', continueAfter=0))
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken':[resumptionToken]}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumptionTokens = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken')
        self.assertEquals(1, len(resumptionTokens))
        self.assertEquals(None, resumptionTokens[0].text)
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'iterData', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals(dict(continueAfter='0', oaiUntil='', prefix='oai_dc', oaiFrom='', sets=None, batchSize=2, shouldCountHits=False), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[-2:]
        self.assertEquals({'recordId':'id:2&2', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))
        self.assertEquals({'recordId':'id:3&3', 'metadataPrefix':'oai_dc'}, _m(recordMethods[1].kwargs))

    def testNoRecordsMatch(self):
        self._addRecords(['id:0'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix':['oai_dc'], 'set': ['does_not_exist']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(['noRecordsMatch'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))

    def testListRecordsUsingXWait(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True)
        self.oaiList.addObserver(self.observer)

        result = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
        result.next()
        self.assertEquals(['getAllPrefixes', 'suspend'], [m.name for m in self.observer.calledMethods])
        self.assertEquals({"clientIdentifier": self.clientId}, self.observer.calledMethods[-1].kwargs)
        self._addRecords(['id:1&1'])
        self.observer.calledMethods.reset()

        header, body = ''.join(compose(result)).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'iterData', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=None, batchSize=2, shouldCountHits=False), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[-1:]
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))

    def testListRecordsWithoutClientIdentifierGeneratesOne(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True)
        self.oaiList.addObserver(self.observer)

        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server'},
            'port':9000,
            'Client': ('127.0.0.1', 1234)
        }
        with stderr_replaced() as s:
            result = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
            result.next()
        self.assertEquals(['getAllPrefixes', 'suspend'], [m.name for m in self.observer.calledMethods])
        self.assertTrue('clientIdentifier' in self.observer.calledMethods[-1].kwargs)
        self.assertEquals(len(str(uuid4())), len(self.observer.calledMethods[-1].kwargs['clientIdentifier']))
        self.assertEquals("X-Meresco-Oai-Client-Identifier not found in HTTP Headers. Generated a uuid for OAI client from 127.0.0.1\n", s.getvalue())

    def testNotSupportedXWait(self):
        self._addRecords(['id:1', 'id:2'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(['badArgument'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))

    def testNotSupportedValueXWait(self):
        self._addRecords(['id:1', 'id:2'])
        self.oaiList = OaiList(batchSize=2, supportXWait=True)
        self.oaiList.addObserver(self.observer)
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['YesPlease']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(['badArgument'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))
        self.assertTrue("only supports 'True' as valid value" in xpath(oai, "/oai:OAI-PMH/oai:error/text()")[0])

    def testFromAndUntil(self):
        self._addRecords(['id:3&3'])
        def selectArguments(oaiFrom, oaiUntil):
            self.observer.calledMethods.reset()
            arguments = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}
            if oaiFrom:
                arguments['from'] = [oaiFrom]
            if oaiUntil:
                arguments['until'] = [oaiUntil]
            header, body = ''.join(compose(self.oaiList.listRecords(arguments=arguments, **self.httpkwargs))).split(CRLF*2)
            oai = parse(StringIO(body))
            self.assertEquals(['getAllPrefixes', 'oaiSelect'], [m.name for m in self.observer.calledMethods][:2])
            selectKwargs = self.observer.calledMethods[1].kwargs
            return selectKwargs['oaiFrom'], selectKwargs['oaiUntil']

        self.assertEquals((None, None), selectArguments(None, None))
        self.assertEquals(('2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z'), selectArguments('2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z'))
        self.assertEquals(('2000-01-01T00:00:00Z', '2000-01-01T23:59:59Z'), selectArguments('2000-01-01', '2000-01-01'))
        self.assertEquals((None, '2000-01-01T00:00:00Z'), selectArguments(None, '2000-01-01T00:00:00Z'))
        self.assertEquals(('2000-01-01T00:00:00Z', None), selectArguments('2000-01-01T00:00:00Z', None))

    def testFromAndUntilErrors(self):
        def getError(oaiFrom, oaiUntil):
            self._addRecords(['id:3&3'])
            self.observer.calledMethods.reset()
            arguments = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}
            if oaiFrom:
                arguments['from'] = [oaiFrom]
            if oaiUntil:
                arguments['until'] = [oaiUntil]
            header, body = ''.join(compose(self.oaiList.listRecords(arguments=arguments, **self.httpkwargs))).split(CRLF*2)
            oai = parse(StringIO(body))
            self.assertEquals(1, len(xpath(oai, '//oai:error')), body)
            error = xpath(oai, '//oai:error')[0]
            return error.attrib['code']

        self.assertEquals('badArgument', getError('thisIsNotEvenADateStamp', 'thisIsNotEvenADateStamp'))
        self.assertEquals('badArgument', getError('2000-01-01T00:00:00Z', '2000-01-01'))
        self.assertEquals('badArgument', getError('2000-01-01T00:00:00Z', '1999-01-01T00:00:00Z'))

    def testConcurrentListRequestsDontInterfere(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True)
        self.oaiList.addObserver(self.observer)

        # ListRecords request
        resultListRecords = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
        resultListRecords.next()

        # ListIdentifiers request
        resultListIdentifiers = compose(self.oaiList.listRecords(arguments={'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))
        resultListIdentifiers.next()

        # resume ListRecords
        self._addRecords(['id:1&1'])
        header, body = ''.join(compose(resultListRecords)).split(CRLF*2)
        self.assertFalse('</ListIdentifiers>' in body, body)
        self.assertTrue('</ListRecords>' in body, body)

    def testXCount(self):
        self._addRecords(['id%s' % i for i in xrange(99)])

        header, body = ''.join(s for s in compose(self.oaiList.listRecords(arguments={'verb': ['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-count': ['True']}, **self.httpkwargs)) if not s is Yield).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        recordsRemaining = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/@recordsRemaining')[0]
        self.assertEquals('97', recordsRemaining)
        continueAfter = self.oaiJazz.getRecord('id97').stamp
        resumptionToken = str(ResumptionToken(metadataPrefix='oai_dc', continueAfter=continueAfter))

        header, body = ''.join(s for s in compose(self.oaiList.listRecords(arguments={'verb': ['ListRecords'], 'resumptionToken': [resumptionToken], 'x-count': ['True']}, **self.httpkwargs)) if not s is Yield).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEquals(1, len(xpath(oai, '//mock:record')))
        self.assertEquals(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/@recordsRemaining')))

        selectMethod = self.observer.calledMethods[1]
        self.assertEquals(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=None, batchSize=2, shouldCountHits=True), selectMethod.kwargs)

    def _addRecords(self, identifiers, sets=None):
        for identifier in identifiers:
            self.oaiJazz.addOaiRecord(identifier=identifier, sets=sets, metadataFormats=[('oai_dc', '', '')])


def xpath(node, path):
    return node.xpath(path, namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/',
        'mock': 'uri:mock',})

def _m(kwargs):
    return {'metadataPrefix': kwargs['metadataPrefix'], 'recordId': kwargs['record'].identifier}

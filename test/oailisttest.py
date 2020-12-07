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
# Copyright (C) 2012-2016, 2018 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 SURFmarket https://surf.nl
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

from io import StringIO
from xml.sax.saxutils import escape as escapeXml
from lxml.etree import parse
from uuid import uuid4
from os.path import join

from seecr.test import SeecrTestCase, CallTrace
from seecr.test.io import stderr_replaced

from weightless.core import compose, Yield, NoneOfTheObserversRespond, asString, consume
from meresco.components.http.utils import CRLF
from meresco.oai.oairepository import OaiRepository
from meresco.sequentialstore import MultiSequentialStorage

from meresco.oai.oailist import OaiList
from meresco.oai import OaiJazz
from meresco.oai.oairecord import OaiRecord
from meresco.xml.namespaces import namespaces

from meresco.oaicommon import ResumptionToken

class OaiListTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.oaiJazz = OaiJazz(self.tempdir)
        self.oaiList = OaiList(batchSize=2, repository=OaiRepository())
        self.observer = CallTrace('observer', emptyGeneratorMethods=['suspendBeforeSelect'])
        self.observer.methods['suspendAfterNoResult'] = lambda **kwargs: (s for s in ['SUSPEND'])
        self.observer.methods['oaiWatermark'] = lambda o=None: (x for x in ["Crafted By Seecr"])
        def oaiRecord(record, metadataPrefix, fetchedRecords=None):
            yield '<mock:record xmlns:mock="uri:mock">%s/%s</mock:record>' % (escapeXml(record.identifier), escapeXml(metadataPrefix))
        self.observer.methods['oaiRecord'] = oaiRecord
        self.observer.methods['oaiRecordHeader'] = oaiRecord
        self.observer.methods['isKnownPrefix'] = self.oaiJazz.isKnownPrefix
        self.observer.methods['oaiSelect'] = self.oaiJazz.oaiSelect
        self.getMultipleDataIdentifiers = []
        def getMultipleData(**kwargs):
            self.getMultipleDataIdentifiers.append(list(kwargs.get('identifiers')))
            raise NoneOfTheObserversRespond('No one', 0)
        self.observer.methods['getMultipleData'] = getMultipleData
        self.oaiList.addObserver(self.observer)
        self.clientId = str(uuid4())
        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers': {'Host':'server', 'X-Meresco-Oai-Client-Identifier': self.clientId},
            'port': 9000,
        }

    def tearDown(self):
        from time import sleep
        sleep(0.05)
        SeecrTestCase.tearDown(self)

    def testListRecords(self):
        self._addRecords(['id:0&0', 'id:1&1'])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEqual(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken')))
        self.assertEqual(['isKnownPrefix', 'oaiSelect', 'oaiWatermark', 'getMultipleData', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEqual(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=[], batchSize=2, shouldCountHits=False, partition=None), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[4:]
        self.assertEqual({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))
        self.assertEqual({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(recordMethods[1].kwargs))
        self.assertEqual([['id:0&0', 'id:1&1']], self.getMultipleDataIdentifiers)

    def testListRecordsXBatchSize(self):
        self.oaiList = OaiList(batchSize=5, repository=OaiRepository())
        self.oaiList.addObserver(self.observer)
        self._addRecords(['id:{0}&{0}'.format(i) for i in range(10)])

        header, body = asString(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-batchSize': ['2']}, **self.httpkwargs)).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumption = xpathFirst(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')

        header, body = asString(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken': [resumption], 'x-batchSize': ['7']}, **self.httpkwargs)).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(7, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumption = xpathFirst(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')

        header, body = asString(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken': [resumption]}, **self.httpkwargs)).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumption = xpathFirst(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')
        self.assertEqual(None, resumption)

    def testListRecordsUsesFetchedRecords(self):
        self._addRecords(['id:0&0', 'id:1'])
        self.observer.methods['getMultipleData'] = lambda name, identifiers, ignoreMissing=False: [('id:0&0', 'data1'), ('id:1', 'data2'), ('id:2', 'data3')]
        consume(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))
        self.assertEqual(['isKnownPrefix', 'oaiSelect', 'oaiWatermark', 'getMultipleData', 'oaiRecord', 'oaiRecord'], self.observer.calledMethodNames())
        self.assertEqual({'id:0&0': 'data1', 'id:1': 'data2', 'id:2': 'data3'}, self.observer.calledMethods[4].kwargs['fetchedRecords'])
        self.assertEqual({'id:0&0': 'data1', 'id:1': 'data2', 'id:2': 'data3'}, self.observer.calledMethods[4].kwargs['fetchedRecords'])

    def testListRecordsWithDeletes(self):
        self._addRecords(['id:0&0', 'id:1&1'])
        consume(self.oaiJazz.delete(identifier='id:1&1'))
        consume(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))
        self.assertEqual([['id:0&0']], self.getMultipleDataIdentifiers)

    def testListRecordsWithMultiSequentialStorage(self):
        oaijazz = OaiJazz(join(self.tempdir, '1'))
        oailist = OaiList(batchSize=2, repository=OaiRepository())
        storage = MultiSequentialStorage(join(self.tempdir, "2"))
        oailist.addObserver(oaijazz)
        oairecord = OaiRecord()
        oailist.addObserver(storage)
        oailist.addObserver(oairecord)
        identifier = "id0"
        oaijazz.addOaiRecord(identifier, (), metadataFormats=[('oai_dc', '', '')])
        storage.addData(identifier=identifier, name="oai_dc", data="data01")
        response = oailist.listRecords(arguments=dict(
                verb=['ListRecords'], metadataPrefix=['oai_dc']), **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEqual("data01", xpath(parse(StringIO(body)), '//oai:metadata')[0].text)

    def testListRecordsWithALotOfDeletedRecords(self):
        oaijazz = OaiJazz(join(self.tempdir, '1'))
        oailist = OaiList(batchSize=2, repository=OaiRepository())
        storage = MultiSequentialStorage(join(self.tempdir, "2"))
        oailist.addObserver(oaijazz)
        oairecord = OaiRecord()
        oailist.addObserver(storage)
        oailist.addObserver(oairecord)
        for id in ['id0', 'id1', 'id1']:
            oaijazz.addOaiRecord(id, (), metadataFormats=[('oai_dc', '', '')])
            storage.addData(identifier=id, name="oai_dc", data="data_%s" % id)
        response = oailist.listRecords(arguments=dict(
                verb=['ListRecords'], metadataPrefix=['oai_dc']), **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEqual(["data_id0", "data_id1"], xpath(parse(StringIO(body)), '//oai:metadata/text()'))

    def testListIdentifiers(self):
        self._addRecords(['id:0&0', 'id:1&1'])

        header, body = ''.join(compose(self.oaiList.listIdentifiers(arguments={'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListIdentifiers/mock:record')))
        self.assertEqual(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken')))
        self.assertEqual(['isKnownPrefix', 'oaiSelect', 'oaiWatermark', 'getMultipleData', 'oaiRecordHeader', 'oaiRecordHeader'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEqual(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=[], batchSize=2, shouldCountHits=False, partition=None), selectMethod.kwargs)
        headerMethods = self.observer.calledMethods[4:]
        self.assertEqual({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, _m(headerMethods[0].kwargs))
        self.assertEqual({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(headerMethods[1].kwargs))

    def testListRecordsProducesResumptionToken(self):
        self._addRecords(['id:0&0', 'id:1&1', 'id:2&2'], sets=[('set0', 'setName')])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'from': ['2000-01-01T00:00:00Z'], 'until': ['4012-01-01T00:00:00Z'], 'set': ['set0']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumptionToken = ResumptionToken.fromString(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')[0])
        self.assertEqual('4012-01-01T00:00:00Z', resumptionToken.until)
        self.assertEqual('2000-01-01T00:00:00Z', resumptionToken.from_)
        self.assertEqual('set0', resumptionToken.set_)
        self.assertEqual('oai_dc', resumptionToken.metadataPrefix)
        continueAfter = self.oaiJazz.getRecord('id:1&1').stamp
        self.assertEqual(str(continueAfter), resumptionToken.continueAfter)
        self.assertEqual(['isKnownPrefix', 'oaiSelect', 'oaiWatermark', 'getMultipleData', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEqual(dict(continueAfter='0', oaiUntil='4012-01-01T00:00:00Z', prefix='oai_dc', oaiFrom='2000-01-01T00:00:00Z', sets=['set0'], batchSize=2, shouldCountHits=False, partition=None), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[4:]
        self.assertEqual({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))
        self.assertEqual({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(recordMethods[1].kwargs))

    def testListRecordsUsesGivenResumptionToken(self):
        self._addRecords(['id:2&2'], sets=[('set0', 'setName')])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken':['u4012-01-01T00:00:00Z|c1000|moai_dc|sset0|f2000-01-01T00:00:00Z']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEqual(['isKnownPrefix', 'oaiSelect', 'oaiWatermark', 'getMultipleData', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEqual(dict(continueAfter='1000', oaiUntil='4012-01-01T00:00:00Z', prefix='oai_dc', oaiFrom='2000-01-01T00:00:00Z', sets=['set0'], batchSize=2, shouldCountHits=False, partition=None), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[4:]
        self.assertEqual({'recordId':'id:2&2', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))

    def testListRecordsEmptyFinalResumptionToken(self):
        self._addRecords(['id:2&2', 'id:3&3'])
        resumptionToken = str(ResumptionToken(metadataPrefix='oai_dc', continueAfter=0))
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken':[resumptionToken]}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumptionTokens = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken')
        self.assertEqual(1, len(resumptionTokens))
        self.assertEqual(None, resumptionTokens[0].text)
        self.assertEqual(['isKnownPrefix', 'oaiSelect', 'oaiWatermark', 'getMultipleData', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEqual(dict(continueAfter='0', oaiUntil='', prefix='oai_dc', oaiFrom='', sets=[], batchSize=2, shouldCountHits=False, partition=None), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[-2:]
        self.assertEqual({'recordId':'id:2&2', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))
        self.assertEqual({'recordId':'id:3&3', 'metadataPrefix':'oai_dc'}, _m(recordMethods[1].kwargs))

    def testNoRecordsMatch(self):
        self._addRecords(['id:0'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix':['oai_dc'], 'set': ['does_not_exist']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(['noRecordsMatch'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))

    def testListRecordsUsingXWait(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True, repository=OaiRepository())
        self.oaiList.addObserver(self.observer)

        result = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
        next(result)
        self.assertEqual(['suspendBeforeSelect', 'isKnownPrefix', 'suspendAfterNoResult'], [m.name for m in self.observer.calledMethods])
        self.assertEqual({"batchSize":2, "clientIdentifier": self.clientId, "prefix": 'oai_dc', 'sets': [], 'oaiFrom': None,  'oaiUntil':None, 'shouldCountHits': False, 'x-wait':True, 'continueAfter': '0', 'partition': None}, self.observer.calledMethods[-1].kwargs)
        self._addRecords(['id:1&1'])
        self.observer.calledMethods.reset()

        header, body = ''.join(compose(result)).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEqual(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')))
        self.assertEqual(['suspendBeforeSelect', 'isKnownPrefix', 'oaiSelect', 'oaiWatermark', 'getMultipleData', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[2]
        self.assertEqual(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=[], batchSize=2, shouldCountHits=False, partition=None), selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[-1:]
        self.assertEqual({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, _m(recordMethods[0].kwargs))

    def testListRecordsWithoutClientIdentifierGeneratesOne(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True, repository=OaiRepository())
        self.oaiList.addObserver(self.observer)

        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server'},
            'port':9000,
            'Client': ('127.0.0.1', 1234)
        }
        with stderr_replaced() as s:
            result = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
            next(result)
        self.assertEqual(['suspendBeforeSelect', 'isKnownPrefix', 'suspendAfterNoResult'], [m.name for m in self.observer.calledMethods])
        self.assertTrue('clientIdentifier' in self.observer.calledMethods[-1].kwargs)
        self.assertEqual(len(str(uuid4())), len(self.observer.calledMethods[-1].kwargs['clientIdentifier']))
        self.assertEqual("X-Meresco-Oai-Client-Identifier not found in HTTP Headers. Generated a uuid for OAI client from 127.0.0.1\n", s.getvalue())

    def testNotSupportedXWait(self):
        self._addRecords(['id:1', 'id:2'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(['badArgument'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))

    def testNotSupportedValueXWait(self):
        self._addRecords(['id:1', 'id:2'])
        self.oaiList = OaiList(batchSize=2, supportXWait=True, repository=OaiRepository())
        self.oaiList.addObserver(self.observer)
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['YesPlease']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEqual(['badArgument'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))
        self.assertTrue("only supports 'True' as valid value" in xpath(oai, "/oai:OAI-PMH/oai:error/text()")[0])

    def testListRecordsWithPartition(self):
        self._addRecords(['id:1', 'id:2'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-partition': ['2/2']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(['id:1/oai_dc'], xpath(oai, '//mock:record/text()'))
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-partition': ['1/2']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(['id:2/oai_dc'], xpath(oai, '//mock:record/text()'))

    @stderr_replaced
    def testListRecordsWithOldPartitionParameter(self):
        self._addRecords(['id:1', 'id:2'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-parthash': ['2/2']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(['id:1/oai_dc'], xpath(oai, '//mock:record/text()'))

    def testListRecordsProducesResumptionTokenWithPartition(self):
        self._addRecords(['id:%s' % i for i in range(10)])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-partition':['1/2']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumptionToken = ResumptionToken.fromString(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')[0])
        self.assertEqual(['id:2/oai_dc', 'id:3/oai_dc'], xpath(oai, '//mock:record/text()'))
        self.assertEqual('1/2', str(resumptionToken.partition))
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken': [str(resumptionToken)]}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(['id:5/oai_dc', 'id:6/oai_dc'], xpath(oai, '//mock:record/text()'))


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
            self.assertEqual(['isKnownPrefix', 'oaiSelect'], [m.name for m in self.observer.calledMethods][:2])
            selectKwargs = self.observer.calledMethods[1].kwargs
            return selectKwargs['oaiFrom'], selectKwargs['oaiUntil']

        self.assertEqual((None, None), selectArguments(None, None))
        self.assertEqual(('2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z'), selectArguments('2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z'))
        self.assertEqual(('2000-01-01T00:00:00Z', '2000-01-01T23:59:59Z'), selectArguments('2000-01-01', '2000-01-01'))
        self.assertEqual((None, '2000-01-01T00:00:00Z'), selectArguments(None, '2000-01-01T00:00:00Z'))
        self.assertEqual(('2000-01-01T00:00:00Z', None), selectArguments('2000-01-01T00:00:00Z', None))

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
            self.assertEqual(1, len(xpath(oai, '//oai:error')), body)
            error = xpath(oai, '//oai:error')[0]
            return error.attrib['code']

        self.assertEqual('badArgument', getError('thisIsNotEvenADateStamp', 'thisIsNotEvenADateStamp'))
        self.assertEqual('badArgument', getError('2000-01-01T00:00:00Z', '2000-01-01'))
        self.assertEqual('badArgument', getError('2000-01-01T00:00:00Z', '1999-01-01T00:00:00Z'))

    def testConcurrentListRequestsDontInterfere(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True, repository=OaiRepository())
        self.oaiList.addObserver(self.observer)

        # ListRecords request
        resultListRecords = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
        next(resultListRecords)

        # ListIdentifiers request
        resultListIdentifiers = compose(self.oaiList.listRecords(arguments={'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))
        next(resultListIdentifiers)

        # resume ListRecords
        self._addRecords(['id:1&1'])
        header, body = ''.join(compose(resultListRecords)).split(CRLF*2)
        self.assertFalse('</ListIdentifiers>' in body, body)
        self.assertTrue('</ListRecords>' in body, body)

    def testXCount(self):
        self._addRecords(['id%s' % i for i in range(99)])

        header, body = ''.join(s for s in compose(self.oaiList.listRecords(arguments={'verb': ['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-count': ['True']}, **self.httpkwargs)) if not s is Yield).split(CRLF*2)
        firstBatch = body
        oai = parse(StringIO(body))
        self.assertEqual(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        recordsRemaining = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/@recordsRemaining')[0]
        self.assertEqual('97', recordsRemaining)
        continueAfter = self.oaiJazz.getRecord('id97').stamp
        resumptionToken = str(ResumptionToken(metadataPrefix='oai_dc', continueAfter=continueAfter))

        header, body = ''.join(s for s in compose(self.oaiList.listRecords(arguments={'verb': ['ListRecords'], 'resumptionToken': [resumptionToken], 'x-count': ['True']}, **self.httpkwargs)) if not s is Yield).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEqual(1, len(xpath(oai, '//mock:record')))
        self.assertEqual(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/@recordsRemaining')))

        selectMethod = self.observer.calledMethods[1]
        self.assertEqual(dict(continueAfter='0', oaiUntil=None, prefix='oai_dc', oaiFrom=None, sets=[], batchSize=2, shouldCountHits=True, partition=None), selectMethod.kwargs)

    def testGetMultipleDataWithOtherBatchSize(self):
        self._addRecords(['id%s' % i for i in range(99)])
        self.oaiList = OaiList(batchSize=10, dataBatchSize=2, repository=OaiRepository())
        self.oaiList.addObserver(self.observer)
        def getMultipleData(identifiers, **kwargs):
            return [(id, '<data id="%s"/>' % id) for id in identifiers]
        self.observer.methods['getMultipleData'] = getMultipleData
        def oaiRecord(record, metadataPrefix, fetchedRecords=None):
            yield fetchedRecords[record.identifier]
        self.observer.methods['oaiRecord'] = oaiRecord

        body = asString(self.oaiList.listRecords(arguments=dict(verb=['ListRecords'], metadataPrefix=['oai_dc']), **self.httpkwargs)).split(CRLF*2,1)[-1]
        oai = parse(StringIO(body))
        self.assertEqual(['id0', 'id1', 'id2', 'id3', 'id4', 'id5', 'id6', 'id7', 'id8', 'id9'], xpath(oai, '//oai:ListRecords/oai:data/@id'))

        self.assertEqual(['isKnownPrefix',
                'oaiSelect',
                'oaiWatermark',
                'getMultipleData',
                'oaiRecord',
                'oaiRecord',
                'getMultipleData',
                'oaiRecord',
                'oaiRecord',
                'getMultipleData',
                'oaiRecord',
                'oaiRecord',
                'getMultipleData',
                'oaiRecord',
                'oaiRecord',
                'getMultipleData',
                'oaiRecord',
                'oaiRecord'
            ], self.observer.calledMethodNames())


    def _addRecords(self, identifiers, sets=None):
        for identifier in identifiers:
            self.oaiJazz.addOaiRecord(identifier=identifier, sets=sets, metadataFormats=[('oai_dc', '', '')])

namespaces = namespaces.copyUpdate({'mock':'uri:mock'})
xpath = namespaces.xpath
xpathFirst = namespaces.xpathFirst

def _m(kwargs):
    return {'metadataPrefix': kwargs['metadataPrefix'], 'recordId': kwargs['record'].identifier}

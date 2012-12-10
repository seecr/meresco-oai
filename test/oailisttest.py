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
# Copyright (C) 2012 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from itertools import imap
from StringIO import StringIO
from xml.sax.saxutils import escape as escapeXml
from lxml.etree import parse
from uuid import uuid4

from seecr.test import SeecrTestCase, CallTrace

from weightless.core import compose, Yield
from meresco.components.http.utils import CRLF

from meresco.oai.oailist import OaiList
from meresco.oai.oairecord import OaiRecord
from meresco.oai.resumptiontoken import resumptionTokenFromString, ResumptionToken
from meresco.oai.oaiutils import OaiException

from mockoaijazz import MockOaiJazz


class OaiListTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.oaiList = OaiList(batchSize=2)
        self.observer = CallTrace('observer')
        self.observer.returnValues['getAllPrefixes'] = ['oai_dc']
        self.observer.methods['oaiSelect'] = lambda **kwargs: (i for i in [])
        self.observer.methods['suspend'] = lambda clientIdentifier: (s for s in ['SUSPEND'])
        self.observer.methods['oaiWatermark'] = lambda o=None: (x for x in ["Crafted By Seecr"])
        def oaiRecord(recordId, metadataPrefix):
            yield '<mock:record xmlns:mock="uri:mock">%s/%s</mock:record>' % (escapeXml(recordId), escapeXml(metadataPrefix))
        self.observer.methods['oaiRecord'] = oaiRecord
        self.observer.methods['oaiRecordHeader'] = oaiRecord
        self.oaiList.addObserver(self.observer)
        self.clientId = str(uuid4())
        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server', 'X-Meresco-Oai-Client-Identifier': self.clientId},
            'port':9000,
        }

    def testListRecords(self):
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:0&0', 'id:1&1'])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEquals(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals({'continueAfter':'0', 'oaiUntil':None, 'prefix':'oai_dc', 'oaiFrom':None, 'sets':None}, selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[3:]
        self.assertEquals({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, recordMethods[0].kwargs)
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, recordMethods[1].kwargs)

    def testListIdentifiers(self):
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:0&0', 'id:1&1'])

        header, body = ''.join(compose(self.oaiList.listIdentifiers(arguments={'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListIdentifiers/mock:record')))
        self.assertEquals(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecordHeader', 'oaiRecordHeader'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals({'continueAfter':'0', 'oaiUntil':None, 'prefix':'oai_dc', 'oaiFrom':None, 'sets':None}, selectMethod.kwargs)
        headerMethods = self.observer.calledMethods[3:]
        self.assertEquals({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, headerMethods[0].kwargs)
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, headerMethods[1].kwargs)

    def testListRecordsProducesResumptionToken(self):
        self.observer.returnValues['getUnique'] = 'unique_for_id'
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:0&0', 'id:1&1', 'id:2&2'])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'from': ['2000-01-01T00:00:00Z'], 'until': ['2012-01-01T00:00:00Z'], 'set': ['set0']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumptionToken = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')
        self.assertEquals(['u2012-01-01T00:00:00Z|cunique_for_id|moai_dc|sset0|f2000-01-01T00:00:00Z'], resumptionToken)
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecord', 'oaiRecord', 'getUnique'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals({'continueAfter':'0', 'oaiUntil':'2012-01-01T00:00:00Z', 'prefix':'oai_dc', 'oaiFrom':'2000-01-01T00:00:00Z', 'sets':['set0']}, selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[3:]
        self.assertEquals({'recordId':'id:0&0', 'metadataPrefix':'oai_dc'}, recordMethods[0].kwargs)
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, recordMethods[1].kwargs)

    def testListRecordsWithResumptionToken(self):
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:2&2'])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken':['u2012-01-01T00:00:00Z|cunique_for_id|moai_dc|sset0|f2000-01-01T00:00:00Z']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEquals(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals({'continueAfter':'unique_for_id', 'oaiUntil':'2012-01-01T00:00:00Z', 'prefix':'oai_dc', 'oaiFrom':'2000-01-01T00:00:00Z', 'sets':['set0']}, selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[3:]
        self.assertEquals({'recordId':'id:2&2', 'metadataPrefix':'oai_dc'}, recordMethods[0].kwargs)

    def testListRecordsEmptyFinalResumptionToken(self):
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:2&2', 'id:3&3'])

        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'resumptionToken':['u2012-01-01T00:00:00Z|cunique_for_id|moai_dc|sset0|f2000-01-01T00:00:00Z']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        resumptionTokens = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken')
        self.assertEquals(1, len(resumptionTokens))
        self.assertEquals(None, resumptionTokens[0].text)
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecord', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals({'continueAfter':'unique_for_id', 'oaiUntil':'2012-01-01T00:00:00Z', 'prefix':'oai_dc', 'oaiFrom':'2000-01-01T00:00:00Z', 'sets':['set0']}, selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[3:]
        self.assertEquals({'recordId':'id:2&2', 'metadataPrefix':'oai_dc'}, recordMethods[0].kwargs)
        self.assertEquals({'recordId':'id:3&3', 'metadataPrefix':'oai_dc'}, recordMethods[1].kwargs)

    def testNoRecordsMatch(self):
        self.observer.returnValues['oaiSelect'] = (f for f in [])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix':['oai_dc']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(['noRecordsMatch'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))

    def testListRecordsUsingXWait(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True)
        self.oaiList.addObserver(self.observer)
        self.observer.returnValues['oaiSelect'] = (f for f in [])

        result = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
        suspend = result.next()
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'suspend'], [m.name for m in self.observer.calledMethods])
        self.assertEquals({"clientIdentifier": self.clientId}, self.observer.calledMethods[2].kwargs)
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:1&1'])
        self.observer.calledMethods.reset()

        header, body = ''.join(compose(result)).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecord', 'getUnique'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals({'continueAfter':'0', 'oaiUntil':None, 'prefix':'oai_dc', 'oaiFrom':None, 'sets':None}, selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[3:]
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'oai_dc'}, recordMethods[0].kwargs)

    def testListRecordsUsingXWaitWhenSetNotFound(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True)
        self.oaiList.addObserver(self.observer)

        result = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['other_prefix'], 'x-wait': ['True']}, **self.httpkwargs))
        suspend = result.next()
        self.assertEquals(['getAllPrefixes', 'suspend'], [m.name for m in self.observer.calledMethods])
        self.observer.returnValues['getAllPrefixes'] = ['other_prefix']
        suspend = result.next() 
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:1&1'])
        self.observer.calledMethods.reset()

        header, body = ''.join(compose(result)).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        self.assertEquals(1, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')))
        self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecord', 'getUnique'], [m.name for m in self.observer.calledMethods])
        selectMethod = self.observer.calledMethods[1]
        self.assertEquals({'continueAfter':'0', 'oaiUntil':None, 'prefix':'other_prefix', 'oaiFrom':None, 'sets':None}, selectMethod.kwargs)
        recordMethods = self.observer.calledMethods[3:]
        self.assertEquals({'recordId':'id:1&1', 'metadataPrefix':'other_prefix'}, recordMethods[0].kwargs)

    def testNotSupportedXWait(self):
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:1', 'id:2'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(['badArgument'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))

    def testNotSupportedValueXWait(self):
        self.oaiList = OaiList(batchSize=2, supportXWait=True)
        self.oaiList.addObserver(self.observer)
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:1', 'id:2'])
        header, body = ''.join(compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['YesPlease']}, **self.httpkwargs))).split(CRLF*2)
        oai = parse(StringIO(body))

        self.assertEquals(['badArgument'], xpath(oai, "/oai:OAI-PMH/oai:error/@code"))
        self.assertTrue("only supports 'True' as valid value" in xpath(oai, "/oai:OAI-PMH/oai:error/text()")[0])

    def testFromAndUntil(self):
        def selectArguments(oaiFrom, oaiUntil):
            self.observer.returnValues['oaiSelect'] = (f for f in ['id:3&3'])
            self.observer.calledMethods.reset()
            arguments = {'verb':['ListRecords'], 'metadataPrefix': ['oai_dc']}
            if oaiFrom:
                arguments['from'] = [oaiFrom]
            if oaiUntil:
                arguments['until'] = [oaiUntil]
            header, body = ''.join(compose(self.oaiList.listRecords(arguments=arguments, **self.httpkwargs))).split(CRLF*2)
            oai = parse(StringIO(body))
            self.assertEquals(0, len(xpath(oai, '//oai:error')), body)
            self.assertEquals(['getAllPrefixes', 'oaiSelect', 'oaiWatermark', 'oaiRecord'], [m.name for m in self.observer.calledMethods])
            selectKwargs = self.observer.calledMethods[1].kwargs
            return selectKwargs['oaiFrom'], selectKwargs['oaiUntil']

        self.assertEquals((None, None), selectArguments(None, None))
        self.assertEquals(('2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z'), selectArguments('2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z'))
        self.assertEquals(('2000-01-01T00:00:00Z', '2000-01-01T23:59:59Z'), selectArguments('2000-01-01', '2000-01-01'))
        self.assertEquals((None, '2000-01-01T00:00:00Z'), selectArguments(None, '2000-01-01T00:00:00Z'))
        self.assertEquals(('2000-01-01T00:00:00Z', None), selectArguments('2000-01-01T00:00:00Z', None))

    def testFromAndUntilErrors(self):
        def getError(oaiFrom, oaiUntil):
            self.observer.returnValues['oaiSelect'] = (f for f in ['id:3&3'])
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
        self.observer.returnValues['oaiSelect'] = (f for f in [])

        # ListRecords request
        resultListRecords = compose(self.oaiList.listRecords(arguments={'verb':['ListRecords'], 'metadataPrefix': ['oai_dc'], 'x-wait': ['True']}, **self.httpkwargs))
        suspend = resultListRecords.next()

        # ListIdentifiers request
        resultListIdentifiers = compose(self.oaiList.listRecords(arguments={'verb':['ListIdentifiers'], 'metadataPrefix': ['oai_dc']}, **self.httpkwargs))
        resultListIdentifiers.next()

        # resume ListRecords
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:1&1'])
        header, body = ''.join(compose(resultListRecords)).split(CRLF*2)
        self.assertFalse('</ListIdentifiers>' in body, body)
        self.assertTrue('</ListRecords>' in body, body)

    def testXCount(self):
        self.observer.returnValues['getUnique'] = 'unique_for_id'
        self.observer.returnValues['oaiSelect'] = ('id%s&%s' % (i, i) for i in xrange(1000))

        header, body = ''.join(s for s in compose(self.oaiList.listRecords(arguments={'verb': ['ListRecords'], 'metadataPrefix': ['oai_dc'], 'from': ['2000-01-01T00:00:00Z'], 'until': ['2012-01-01T00:00:00Z'], 'set': ['set0'], 'x-count': ['True']}, **self.httpkwargs)) if not s is Yield).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEquals(2, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/mock:record')))
        recordsRemaining = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/@recordsRemaining')[0]
        self.assertEquals('998', recordsRemaining)
        resumptionToken = xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')[0]
        self.observer.returnValues['oaiSelect'] = (f for f in ['id:999&999'])
        header, body = ''.join(s for s in compose(self.oaiList.listRecords(arguments={'verb': ['ListRecords'], 'resumptionToken': [resumptionToken], 'x-count': ['True']}, **self.httpkwargs)) if not s is Yield).split(CRLF*2)
        oai = parse(StringIO(body))
        self.assertEquals(0, len(xpath(oai, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/@recordsRemaining')))



def xpath(node, path):
    return node.xpath(path, namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/',
        'mock': 'uri:mock',})
        

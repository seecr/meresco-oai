## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2011-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012, 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from lxml.etree import parse
from meresco.components import lxmltostring, Schedule
from StringIO import StringIO
from os.path import join, isfile
from urllib import urlencode
from simplejson import load
from datetime import datetime

from seecr.test import SeecrTestCase, CallTrace
from seecr.test.io import stdout_replaced
from weightless.core import compose, consume
from weightless.io import Suspend

from meresco.core import asyncreturn
from meresco.oai import OaiDownloadProcessor


class OaiDownloadProcessorTest(SeecrTestCase):
    def testRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        self.assertEquals("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testUpdateRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.setPath('/otherOai')
        oaiDownloadProcessor.setMetadataPrefix('otherPrefix')
        self.assertEquals("""GET /otherOai?verb=ListRecords&metadataPrefix=otherPrefix&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testRequestWithAdditionalHeaders(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        request = oaiDownloadProcessor.buildRequest(additionalHeaders={'Host': 'example.org'})
        self.assertEquals("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nHost: example.org\r\n\r\n""" % oaiDownloadProcessor._identifier, request)

    def testListIdentifiersRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, verb='ListIdentifiers')
        self.assertEquals("""GET /oai?verb=ListIdentifiers&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testSetInRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", set="setName", workingDirectory=self.tempdir, xWait=True)
        self.assertEquals("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&set=setName&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", set="set-_.!~*'()", workingDirectory=self.tempdir, xWait=True)
        self.assertEquals("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&set=set-_.%%21%%7E%%2A%%27%%28%%29&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", set="setName", workingDirectory=self.tempdir, xWait=True)
        self.assertEquals("""GET /oai?verb=ListRecords&resumptionToken=u%%7Cc1286437597991025%%7Cmprefix%%7Cs%%7Cf&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testHandle(self):
        observer = CallTrace(methods={'add': lambda **kwargs: (x for x in [])})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.addObserver(observer)
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '')))))
        self.assertEquals(['add'], [m.name for m in observer.calledMethods])
        self.assertEquals(0, len(observer.calledMethods[0].args))
        self.assertEqualsWS(ONE_RECORD, lxmltostring(observer.calledMethods[0].kwargs['lxmlNode']))
        self.assertEquals('2011-08-22T07:34:00Z', observer.calledMethods[0].kwargs['datestamp'])
        self.assertEquals('oai:identifier:1', observer.calledMethods[0].kwargs['identifier'])

    def testListIdentifiersHandle(self):
        observer = CallTrace(methods={'add': lambda **kwargs: (x for x in [])})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, verb='ListIdentifiers')
        oaiDownloadProcessor.addObserver(observer)
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTIDENTIFIERS_RESPONSE)))))
        self.assertEquals(['add'], [m.name for m in observer.calledMethods])
        self.assertEquals(0, len(observer.calledMethods[0].args))
        self.assertEqualsWS(ONE_HEADER, lxmltostring(observer.calledMethods[0].kwargs['lxmlNode']))
        self.assertEquals('2011-08-22T07:34:00Z', observer.calledMethods[0].kwargs['datestamp'])
        self.assertEquals('oai:identifier:1', observer.calledMethods[0].kwargs['identifier'])

    def testHandleWithTwoRecords(self):
        observer = CallTrace(methods={'add': lambda **kwargs: (x for x in [])})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.addObserver(observer)
        secondRecord = '<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:2</identifier><datestamp>2011-08-22T07:41:00Z</datestamp></header><metadata>ignored</metadata></record>'
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % secondRecord)))))
        self.assertEquals(['add', 'add'], [m.name for m in observer.calledMethods])
        self.assertEquals(0, len(observer.calledMethods[0].args))
        self.assertEqualsWS(ONE_RECORD, lxmltostring(observer.calledMethods[0].kwargs['lxmlNode']))
        self.assertEquals('2011-08-22T07:34:00Z', observer.calledMethods[0].kwargs['datestamp'])
        self.assertEquals('oai:identifier:1', observer.calledMethods[0].kwargs['identifier'])
        self.assertEqualsWS(secondRecord, lxmltostring(observer.calledMethods[1].kwargs['lxmlNode']))
        self.assertEquals('2011-08-22T07:41:00Z', observer.calledMethods[1].kwargs['datestamp'])
        self.assertEquals('oai:identifier:2', observer.calledMethods[1].kwargs['identifier'])

    def testRaiseErrorOnBadArguments(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        badRecord = '<record>No Header</record>'
        try:
            list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % badRecord)))))
            self.fail()
        except IndexError:
            pass

    def testListRecordsRequestError(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        self.assertEquals('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())
        consume(oaiDownloadProcessor.handle(parse(StringIO(ERROR_RESPONSE))))
        self.assertEquals(0, len(observer.calledMethods))
        self.assertEquals("someError: Some error occurred.\n", oaiDownloadProcessor._err.getvalue())
        self.assertEquals('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('metadataPrefix', 'oai_dc'), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())

    def testUseResumptionToken(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        self.assertEquals('x?y&z', oaiDownloadProcessor._resumptionToken)
        self.assertEquals('GET /oai?verb=ListRecords&resumptionToken=x%%3Fy%%26z&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n' % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEquals('x?y&z', oaiDownloadProcessor._resumptionToken)

    def testReadResumptionTokenFromStateWithNewline(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEquals(resumptionToken, oaiDownloadProcessor._resumptionToken)

    def testReadResumptionTokenWhenNoState(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEquals('', oaiDownloadProcessor._resumptionToken)

    def testReadInvalidState(self):
        open(join(self.tempdir, 'harvester.state'), 'w').write("invalid")
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEquals('', oaiDownloadProcessor._resumptionToken)

    def testKeepResumptionTokenOnFailingAddCall(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        observer = CallTrace()
        observer.exceptions={'add': Exception("Could be anything")}
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        self.assertEquals('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())
        self.assertRaises(Exception, lambda: list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))))
        self.assertEquals(['add'], [m.name for m in observer.calledMethods])
        errorOutput = oaiDownloadProcessor._err.getvalue()
        self.assertTrue(errorOutput.startswith('Traceback'), errorOutput)
        self.assertTrue(errorOutput.endswith('Exception: Could be anything\nWhile processing:\n<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:1</identifier><datestamp>2011-08-22T07:34:00Z</datestamp></header><metadata>ignored</metadata></record>%s\n  \n'), errorOutput)
        self.assertEquals('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())

    def testHandleYieldsAtLeastOnceAfterEachRecord(self):
        @asyncreturn
        def add(**kwargs):
            pass
        observer = CallTrace(methods={'add': add})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.addObserver(observer)
        yields = list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '')))))
        self.assertEquals(1, len(yields))

        secondRecord = '<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:2</identifier><datestamp>2011-08-22T07:41:00Z</datestamp></header><metadata>ignored</metadata></record>'
        yields = list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % secondRecord)))))
        self.assertEquals(2, len(yields))

    def testYieldSuspendFromAdd(self):
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.addObserver(observer)
        suspend = Suspend()
        observer.returnValues['add'] = (x for x in [suspend])
        yields = list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '')))))
        self.assertEquals([suspend, None], yields)

    def testHarvesterState(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        state = oaiDownloadProcessor.getState()
        self.assertEquals("x?y&z", state.resumptionToken)
        self.assertEquals(None, state.errorState)

        oaiDownloadProcessor2 = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        state2 = oaiDownloadProcessor2.getState()
        self.assertEquals("x?y&z", state2.resumptionToken)
        self.assertEquals(None, state2.errorState)

    def testHarvesterStateWithError(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        observer = CallTrace()
        observer.exceptions={'add': Exception("Could be anything")}
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO(), name="Name")
        oaiDownloadProcessor.addObserver(observer)
        self.assertRaises(Exception, lambda: list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))))
        state = oaiDownloadProcessor.getState()
        self.assertEquals(resumptionToken, state.resumptionToken)
        self.assertEquals("ERROR while processing 'oai:identifier:1': Could be anything", state.errorState)
        self.assertEquals("Name", state.name)

        oaiDownloadProcessor2 = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        state2 = oaiDownloadProcessor2.getState()
        self.assertEquals(resumptionToken, state2.resumptionToken)
        self.assertEquals("ERROR while processing 'oai:identifier:1': Could be anything", state2.errorState)

    def testPersistentIdentifier(self):
        identifierFilepath = join(self.tempdir, 'harvester.identifier')
        self.assertFalse(isfile(identifierFilepath))
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        currentIdentifier = oaiDownloadProcessor._identifier
        self.assertTrue(isfile(identifierFilepath))
        self.assertEquals(currentIdentifier, open(identifierFilepath).read())
        self.assertEquals("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % currentIdentifier, oaiDownloadProcessor.buildRequest())

        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        self.assertEquals("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\n\r\n""" % currentIdentifier, oaiDownloadProcessor.buildRequest())

    @stdout_replaced
    def testShutdownPersistsStateOnAutocommit(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, autoCommit=False)
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        state = oaiDownloadProcessor.getState()
        self.assertFalse(isfile(join(self.tempdir, 'harvester.state')))

        oaiDownloadProcessor.handleShutdown()
        self.assertEquals({"errorState": None, 'from': '2002-06-01T19:20:30Z', "resumptionToken": state.resumptionToken}, load(open(join(self.tempdir, 'harvester.state'))))

    def testResponseDateAsFrom(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        self.assertEquals('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)

        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        self.assertEquals('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)

    def testBuildRequestNoneWhenNoResumptionToken(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEquals(None, oaiDownloadProcessor._resumptionToken)
        self.assertEquals(None, oaiDownloadProcessor.buildRequest())

    def testRestartAfterFinish(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), restartAfterFinish=True)
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEquals(None, oaiDownloadProcessor._resumptionToken)
        request = oaiDownloadProcessor.buildRequest()
        self.assertTrue(request.startswith('GET /oai?verb=ListRecords&metadataPrefix=oai_dc HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: '), request)

    def testIncrementalHarvestWithFromAfterSomePeriod(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), incrementalHarvestSchedule=Schedule(period=10))
        oaiDownloadProcessor._time = lambda: 1.0
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEquals(None, oaiDownloadProcessor._resumptionToken)
        self.assertEquals('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)

        self.assertEquals(None, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor._time = lambda: 6.0
        self.assertEquals(None, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor._time = lambda: 10.0
        self.assertEquals(None, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor._time = lambda: 11.1
        request = oaiDownloadProcessor.buildRequest()
        self.assertTrue(request.startswith('GET /oai?verb=ListRecords&from=2002-06-01T19%3A20%3A30Z&metadataPrefix=oai_dc'), request)

    def testIncrementalHarvestWithFromWithDefaultScheduleMidnight(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor._time = oaiDownloadProcessor._incrementalHarvestSchedule._time = lambda: 01 * 60 * 60
        oaiDownloadProcessor._incrementalHarvestSchedule._utcnow = lambda: datetime.strptime("01:00", "%H:%M")
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEquals(None, oaiDownloadProcessor._resumptionToken)
        self.assertEquals(24 * 60 * 60.0, oaiDownloadProcessor._incrementalHarvestTime)

    def testIncrementalHarvestScheduleFalse(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), incrementalHarvestSchedule=False)
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEquals(None, oaiDownloadProcessor._resumptionToken)
        self.assertEquals('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)
        self.assertEquals(None, oaiDownloadProcessor._incrementalHarvestTime)


ONE_RECORD = '<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:1</identifier><datestamp>2011-08-22T07:34:00Z</datestamp></header><metadata>ignored</metadata></record>'

LISTRECORDS_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15"
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <ListRecords>
    %s
  </ListRecords>
</OAI-PMH>
""" % (ONE_RECORD + "%s")

ERROR_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15"
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <error code="someError">Some error occurred.</error>
</OAI-PMH>
"""

RESUMPTION_TOKEN = """<resumptionToken expirationDate="2002-06-01T23:20:00Z"
      completeListSize="6"
      cursor="0">x?y&amp;z</resumptionToken>"""

ONE_HEADER = '<header xmlns="http://www.openarchives.org/OAI/2.0/"><identifier>oai:identifier:1</identifier><datestamp>2011-08-22T07:34:00Z</datestamp></header>'

LISTIDENTIFIERS_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15"
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <ListIdentifiers>
    %s
  </ListIdentifiers>
</OAI-PMH>
""" % ONE_HEADER


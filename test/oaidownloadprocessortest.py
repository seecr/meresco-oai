## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2011 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
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

from __future__ import with_statement
from contextlib import contextmanager
from random import randint
from threading import Event, Thread
from time import sleep
from socket import socket, error as SocketError
from lxml.etree import tostring, parse
from StringIO import StringIO
from os.path import join
from urllib import urlencode

from cq2utils import CQ2TestCase, CallTrace
from meresco.core import Observable, be
from meresco.components.http.utils import CRLF
from meresco.oai import OaiDownloadProcessor

from weightless.core import compose

class OaiDownloadProcessorTest(CQ2TestCase):
    def testRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        self.assertEquals("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\n\r\n""", oaiDownloadProcessor.buildRequest())

    def testHandle(self): 
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.addObserver(observer)
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '')))))
        self.assertEquals(['add'], [m.name for m in observer.calledMethods])
        self.assertEquals(0, len(observer.calledMethods[0].args))
        self.assertEqualsWS(ONE_RECORD, tostring(observer.calledMethods[0].kwargs['lxmlNode']))

    def testHandleWithTwoRecords(self): 
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.addObserver(observer)
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '<record>another record</record>')))))
        self.assertEquals(['add', 'add'], [m.name for m in observer.calledMethods])
        self.assertEquals(0, len(observer.calledMethods[0].args))
        self.assertEqualsWS(ONE_RECORD, tostring(observer.calledMethods[0].kwargs['lxmlNode']))
        self.assertEqualsWS('<record xmlns="http://www.openarchives.org/OAI/2.0/">another record</record>', tostring(observer.calledMethods[1].kwargs['lxmlNode']))

    def testListRecordsRequestError(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        self.assertEquals('GET /oai?%s HTTP/1.0\r\n\r\n' % urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor.buildRequest())
        list(oaiDownloadProcessor.handle(parse(StringIO(ERROR_RESPONSE))))
        self.assertEquals(0, len(observer.calledMethods))
        self.assertEquals("someError: Some error occurred.\n", oaiDownloadProcessor._err.getvalue())
        self.assertEquals('GET /oai?%s HTTP/1.0\r\n\r\n' % urlencode([('verb', 'ListRecords'), ('metadataPrefix', 'oai_dc'), ('x-wait', 'True')]), oaiDownloadProcessor.buildRequest())

    def testUseResumptionToken(self):
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        list(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        self.assertEquals('x?y&z', oaiDownloadProcessor._resumptionToken)
        self.assertEquals('Resumptiontoken: x?y&z', open(oaiDownloadProcessor._stateFilePath).read())
        self.assertEquals('GET /oai?verb=ListRecords&resumptionToken=x%3Fy%26z&x-wait=True HTTP/1.0\r\n\r\n', oaiDownloadProcessor.buildRequest())
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
        self.assertEquals('GET /oai?%s HTTP/1.0\r\n\r\n' % urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor.buildRequest())
        self.assertRaises(Exception, lambda: list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))))
        self.assertEquals(['add'], [m.name for m in observer.calledMethods])
        self.assertEquals("", oaiDownloadProcessor._err.getvalue())
        self.assertEquals('GET /oai?%s HTTP/1.0\r\n\r\n' % urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor.buildRequest())


ONE_RECORD = '<record xmlns="http://www.openarchives.org/OAI/2.0/">ignored</record>'

LISTRECORDS_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15" 
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <ListRecords>
    <record>ignored</record>
    %s
  </ListRecords>
</OAI-PMH>
"""

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


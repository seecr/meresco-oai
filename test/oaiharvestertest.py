## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010 Stichting Kennisnet Ict op school.
#       http://www.kennisnetictopschool.nl
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

from __future__ import with_statement
from contextlib import contextmanager
from random import randint
from threading import Event, Thread
from time import sleep
from socket import socket, error as SocketError
from lxml.etree import tostring
from StringIO import StringIO
from os.path import join

from cq2utils import CQ2TestCase, CallTrace
from meresco.core import Observable, be
from meresco.oai import OaiHarvester

@contextmanager
def server(responses, bufsize=4096):
    port = randint(2**10, 2**16)
    start = Event()
    messages = []
    def serverThread():
        s = socket()
        s.bind(('127.0.0.1', port))
        s.listen(0)
        start.set()
        for response in responses:
            connection, address = s.accept()
            msg = connection.recv(bufsize)
            messages.append(msg)
            connection.send(response)
            connection.close()
    thread = Thread(None, serverThread)
    thread.start()
    start.wait()
    yield port, messages
    thread.join()


class OaiHarvesterTest(CQ2TestCase):
    def testOne(self):
        reactor = CallTrace("reactor")
        with server([RESPONSE]) as (port, msgs):
            harvester, observer, reactor = self.getHarvester("localhost", port, "/oai", 'dc')
            self.assertEquals('addTimer', reactor.calledMethods[0].name)
            self.assertEquals(1, reactor.calledMethods[0].args[0])
            callback = reactor.calledMethods[0].args[1]
            callback() # connect
            self.assertEquals('addWriter', reactor.calledMethods[1].name)
            callback = reactor.calledMethods[1].args[1]
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET", msgs[0][:3])
            self.assertEquals('removeWriter', reactor.calledMethods[2].name)
            self.assertEquals('addReader', reactor.calledMethods[3].name)
            callback = reactor.calledMethods[3].args[1]
            callback() # sok.recv
            callback() # sok.recv
            self.assertEquals('add', observer.calledMethods[0].name)
            self.assertEqualsWS(BODY, tostring(observer.calledMethods[0].args[0]))

    def testNoConnectionPossible(self):
        harvester, observer, reactor = self.getHarvester("some.nl", 'no-port', "/oai", 'dc')
        callback = reactor.calledMethods[0].args[1]
        try:
            callback() # connect
            self.fail()
        except TypeError, e:
            self.assertEquals("an integer is required", str(e))

    def testInvalidPortConnectionRefused(self):
        harvester, observer, reactor = self.getHarvester("localhost", 88, "/oai", 'dc')
        callback = reactor.calledMethods[0].args[1]
        callback() # connect
        self.assertEquals("addWriter", reactor.calledMethods[1].name)
        callback() # connect
        self.assertEquals("removeWriter", reactor.calledMethods[2].name)
        self.assertEquals("addTimer", reactor.calledMethods[3].name)
        self.assertEquals("Connection to localhost:88/oai refused.\n", self._err.getvalue())

    def testInvalidHost(self):
        harvester, observer, reactor = self.getHarvester("UEYR^$*FD(#>NDJ.khfd9.(*njnd", 88, "/oai", 'dc')
        callback = reactor.calledMethods[0].args[1]
        callback() # connect
        self.assertEquals('addTimer', reactor.calledMethods[-1].name)
        self.assertEquals("-2: Name or service not known\n", self._err.getvalue())

    def testInvalidHostConnectionRefused(self):
        harvester, observer, reactor = self.getHarvester("127.0.0.255", 9876, "/oai", 'dc')
        callback = reactor.calledMethods[0].args[1]
        callback()
        self.assertEquals("addWriter", reactor.calledMethods[1].name)
        callback()
        self.assertEquals("removeWriter", reactor.calledMethods[2].name)
        self.assertEquals("addTimer", reactor.calledMethods[3].name)

    def testSuccess(self):
        with server([RESPONSE]) as (port, msgs):
            harvester, observer, reactor = self.getHarvester("localhost", port, "/", "dc")
            callback = self.doConnect()
            callback() # HTTP GET
            sleep(0.01)
            callback = reactor.calledMethods[3].args[1]
            callback() # sok.recv
            callback() # recv = ''
            self.assertEquals('add', observer.calledMethods[0].name)
            self.assertEqualsWS(BODY, tostring(observer.calledMethods[0].args[0]))
            self.assertEquals('removeReader', reactor.calledMethods[4].name)
            self.assertEquals('addTimer', reactor.calledMethods[-1].name)

    def testListRecordsRequest(self):
        with server([LISTRECORDS_RESPONSE % '']) as (port, msgs):
            harvester, observer, reactor = self.getHarvester('localhost', port, '/oai', 'dc', xWait=False)
            callback = self.doConnect()
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&metadataPrefix=dc HTTP/1.0\r\n\r\n", msgs[0])

    def testListRecordsRequestError(self):
        with server([ERROR_RESPONSE]) as (port, msgs):
            harvester, observer, reactor = self.getHarvester('localhost', port, '/oai', 'dc')
            callback = self.doConnect()
            callback() # HTTP GET
            sleep(0.01)
            callback() # sok.recv
            callback() # sok.recv == ''
            self.assertEquals("someError: Some error occurred.\n", self._err.getvalue())
            self.assertEquals(0, len(observer.calledMethods))
            self.assertEquals('addTimer', reactor.calledMethods[-1].name)
    
    def testUseResumptionToken(self):
        with server([LISTRECORDS_RESPONSE % RESUMPTION_TOKEN, LISTRECORDS_RESPONSE % ""]) as (port, msgs):
            harvester, observer, reactor = self.getHarvester('localhost', port, '/oai', 'dc')
            callback = self.doConnect()
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&metadataPrefix=dc&x-wait=True HTTP/1.0\r\n\r\n", msgs[0])
            callback() # sok.recv
            callback() # sok.recv == ''
            self.assertEquals(['add'], [m.name for m in observer.calledMethods])
            self.assertEquals('addTimer', reactor.calledMethods[-1].name)
            self.assertEquals('Resumptiontoken: xyz', open(self._harvester._stateFilePath).read())
            callback() # (re)connect
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&resumptionToken=xyz&x-wait=True HTTP/1.0\r\n\r\n", msgs[1])
            callback() # sok.recv
            callback() # sok.recv == ''
            self.assertEquals('Resumptiontoken: ', open(self._harvester._stateFilePath).read())

    def testKeepResumptionTokenOnRestart(self):
        with server([LISTRECORDS_RESPONSE % RESUMPTION_TOKEN]) as (port, msgs):
            harvester, observer, reactor = self.getHarvester('localhost', port, '/oai', 'dc')
            callback = self.doConnect()
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&metadataPrefix=dc&x-wait=True HTTP/1.0\r\n\r\n", msgs[0])
            callback() # sok.recv
            callback() # sok.recv == ''
            self.assertEquals('Resumptiontoken: xyz', open(self._harvester._stateFilePath).read())
        with server([LISTRECORDS_RESPONSE % RESUMPTION_TOKEN]) as (port, msgs):
            harvester, observer, reactor = self.getHarvester('localhost', port, '/oai', 'dc')
            callback = self.doConnect()
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&resumptionToken=xyz&x-wait=True HTTP/1.0\r\n\r\n", msgs[0])

    def testKeepResumptionTokenOnInvalidResponse(self):
        with server([LISTRECORDS_RESPONSE % RESUMPTION_TOKEN, STATUSLINE + 'not XML']) as (port, msgs):
            harvester, observer, reactor = self.getHarvester('localhost', port, '/oai', 'dc')
            callback = self.doConnect()
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&metadataPrefix=dc&x-wait=True HTTP/1.0\r\n\r\n", msgs[0])
            callback() # sok.recv
            callback() # soc.recv == ''
            self.assertEquals('Resumptiontoken: xyz', open(self._harvester._stateFilePath).read())
            callback() # (re)connect
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&resumptionToken=xyz&x-wait=True HTTP/1.0\r\n\r\n", msgs[-1])
            callback() # sok.recv
            callback() # sok.recv == ''
            self.assertTrue("XMLSyntaxError: Start tag expected, '<' not found, line 1, column 1" in self._err.getvalue(), self._err.getvalue())
            self.assertEquals('Resumptiontoken: xyz', open(self._harvester._stateFilePath).read())
            callback() # (re)connect
            callback() # HTTP GET
            sleep(0.01)
            self.assertEquals("GET /oai?verb=ListRecords&resumptionToken=xyz&x-wait=True HTTP/1.0\r\n\r\n", msgs[-1])

    def testReadResumptionTokenFromState(self):
        harvester, observer, reactor = self.getHarvester("localhost", 99999, "/", "prefix")
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(harvester._stateFilePath, 'w').write("Resumptiontoken: %s" % resumptionToken)
        self.assertEquals(resumptionToken, harvester._readState())

    def testReadResumptionTokenWhenNoState(self):
        harvester, observer, reactor = self.getHarvester("localhost", 99999, "/", "prefix")
        self.assertEquals("", harvester._readState())

    def testReadInvalidState(self):
        harvester, observer, reactor = self.getHarvester("localhost", 99999, "/", "prefix")
        open(harvester._stateFilePath, 'w').write("invalid")
        self.assertEquals("", harvester._readState())

    def getHarvester(self, host, port, path, metadataPrefix, workingDir=None, xWait=True):
        if workingDir == None:
            workingDir = join(self.tempdir, 'harvesterstate')
        self._err = StringIO()
        self._reactor = CallTrace("reactor")
        self._harvester = OaiHarvester(self._reactor, host, port, path, metadataPrefix, workingDir=workingDir, xWait=xWait)
        self._harvester._logError = lambda s: self._err.write(s + '\n')
        self._observer = CallTrace("observer")
        self._harvester.addObserver(self._observer)
        self._harvester.observer_init()
        return self._harvester, self._observer, self._reactor

    def doConnect(self):
        callback = self._reactor.calledMethods[0].args[1]
        callback() # connect
        callback = self._reactor.calledMethods[1].args[1]
        return callback

STATUSLINE = """HTTP/1.0 200 OK \r\n\r\n"""
BODY = "<body>BODY</body>"
RESPONSE = STATUSLINE + BODY

LISTRECORDS_RESPONSE = STATUSLINE + """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15" 
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <ListRecords>
    <record>ignored</record>
    %s
  </ListRecords>
</OAI-PMH>
"""

ERROR_RESPONSE = STATUSLINE + """<?xml version="1.0" encoding="UTF-8" ?>
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
      cursor="0">xyz</resumptionToken>"""


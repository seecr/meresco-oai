## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2010-2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010-2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2011-2013, 2015, 2017-2018 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from os.path import join
from random import randint
from threading import Thread
from time import sleep
from urllib2 import urlopen, URLError
from uuid import uuid4

from lucene import getVMEnv

from meresco.core import Observable
from meresco.components.http import ObservableHttpServer
from meresco.components.http.utils import CRLF
from meresco.components import XmlParseLxml, PeriodicDownload
from meresco.oai import OaiPmh, OaiJazz, SuspendRegister
from meresco.oaicommon import OaiDownloadProcessor
from meresco.xml import xpathFirst
from meresco.sequentialstore import MultiSequentialStorage

from seecr.test import SeecrTestCase, CallTrace
from seecr.test.utils import getRequest, sleepWheel
from seecr.test.io import stderr_replaced
from weightless.io import Reactor
from weightless.core import be, compose

from meresco.components import lxmltostring
from StringIO import StringIO
from lxml.etree import XML
from traceback import print_exc


class OaiIntegrationTest(SeecrTestCase):
    def testNearRealtimeOai(self):
        self.run = True
        portNumber = randint(50000, 60000)
        suspendRegister = SuspendRegister()
        oaiJazz = OaiJazz(join(self.tempdir, 'oai'))
        oaiJazz.addObserver(suspendRegister)
        storageComponent = MultiSequentialStorage(join(self.tempdir, 'storage'))
        self._addOaiRecords(storageComponent, oaiJazz, 3)
        oaiPmhThread = Thread(None, lambda: self.startOaiPmh(portNumber, oaiJazz, storageComponent, suspendRegister))

        observer = CallTrace("observer", ignoredAttributes=["observer_init"], methods={'add': lambda **kwargs: (x for x in [])})
        harvestThread = Thread(None, lambda: self.startOaiHarvester(portNumber, observer))

        oaiPmhThread.start()
        harvestThread.start()

        try:
            requests = 3
            sleepWheel(1.0 + 1.0 * requests)

            self.assertEquals(['startOaiBatch', 'add', 'add', 'stopOaiBatch', 'startOaiBatch', 'add', 'stopOaiBatch'], [m.name for m in observer.calledMethods])
            ids = [xpath(m.kwargs['lxmlNode'], '//oai:header/oai:identifier/text()') for m in observer.calledMethods if m.name == 'add']
            self.assertEquals([['id0'],['id1'],['id2']], ids)

            self.assertEquals(1, len(suspendRegister))
            observer.calledMethods.reset()

            requests += 1
            storageComponent.addData(identifier="id3", name="prefix", data="<a>a3</a>")
            oaiJazz.addOaiRecord(identifier="id3", sets=[], metadataFormats=[("prefix", "", "")])
            sleepWheel(1)

            self.assertEquals(0, len(suspendRegister))
            self.assertEquals(['startOaiBatch', 'add', 'stopOaiBatch'], [m.name for m in observer.calledMethods])
            kwarg = lxmltostring(observer.calledMethods[1].kwargs['lxmlNode'])
            self.assertTrue("id3" in kwarg, kwarg)
            sleepWheel(1.0)
            self.assertEquals(1, len(suspendRegister))
        finally:
            self.run = False
            oaiPmhThread.join()
            harvestThread.join()
            oaiJazz.close()

    def testShouldRaiseExceptionOnSameRequestTwice(self):
        self.run = True
        portNumber = randint(50000, 60000)
        oaiJazz = OaiJazz(join(self.tempdir, 'oai'))
        suspendRegister = SuspendRegister()
        oaiJazz.addObserver(suspendRegister)
        storageComponent = MultiSequentialStorage(join(self.tempdir, 'storage'))
        clientId = str(uuid4())

        responses = []
        def doOaiListRecord(port):
            header, body = getRequest(port=portNumber, path="/", arguments={"verb": "ListRecords", "metadataPrefix": "prefix", "x-wait": "True"}, additionalHeaders={'X-Meresco-Oai-Client-Identifier': clientId}, parse=False)
            responses.append((header, body))

        oaiPmhThread = Thread(None, lambda: self.startOaiPmh(portNumber, oaiJazz, storageComponent, suspendRegister))
        harvestThread1 = Thread(None, lambda: doOaiListRecord(portNumber))
        harvestThread2 = Thread(None, lambda: doOaiListRecord(portNumber))

        with stderr_replaced():
            oaiPmhThread.start()
            harvestThread1.start()
            try:
                while len(suspendRegister) == 0:
                    sleep(0.01)
                harvest1Suspend = suspendRegister._suspendObject(clientId)
                self.assertTrue(harvest1Suspend is not None)
                harvestThread2.start()
                while harvest1Suspend == suspendRegister._suspendObject(clientId):
                    sleep(0.01)
                sleep(0.01)
                self.assertTrue(clientId in suspendRegister)
                self.assertTrue(harvest1Suspend != suspendRegister._suspendObject(clientId))

                self.assertEquals(1, len(responses))
                header, body = responses[0]
                self.assertTrue('204' in header, header)
                self.assertTrue(body.startswith('Aborting suspended request'), body)

                storageComponent.addData(identifier="id1", name="prefix", data="<a>a1</a>")
                oaiJazz.addOaiRecord(identifier="id1", sets=[], metadataFormats=[("prefix", "", "")])
                sleep(0.1)

            finally:
                self.run = False
                oaiPmhThread.join()
                harvestThread1.join()
                harvestThread2.join()
                oaiJazz.close()

    def testShouldNotStartToLoopLikeAMadMan(self):
        self.run = True
        portNumber = randint(50000, 60000)
        oaiJazz = OaiJazz(join(self.tempdir, 'oai'))
        suspendRegister = SuspendRegister(maximumSuspendedConnections=5)
        oaiJazz.addObserver(suspendRegister)
        storageComponent = MultiSequentialStorage(join(self.tempdir, 'storage'))

        # def doOaiListRecord(port):
        #     header, body = getRequest(port=portNumber, path="/", arguments={"verb": "ListRecords", "metadataPrefix": "prefix", "x-wait": "True"}, parse=False)

        def doUrlOpenWithTimeout(port, basket):
            try:
                response = urlopen("http://localhost:%s/?verb=ListRecords&metadataPrefix=prefix&x-wait=True" % port, timeout=0.5)
            except URLError, e:
                self.assertTrue('urlopen error timed out>' in str(e), str(e))
            basket.append(response.getcode())

        oaiPmhThread = Thread(None, lambda: self.startOaiPmh(portNumber, oaiJazz, storageComponent, suspendRegister))
        threads = []
        todo = [doUrlOpenWithTimeout] * 7

        statusCodes = []
        oaiPmhThread.start()
        with stderr_replaced():
            while todo:
                func = todo.pop()
                harvestThread = Thread(None, lambda: func(portNumber, statusCodes))
                threads.append(harvestThread)
                harvestThread.start()

            try:
                while len(suspendRegister) == 0:
                    sleep(0.01)
            finally:
                for t in threads:
                    t.join()
                self.run = False
                oaiPmhThread.join()
                oaiJazz.close()

        self.assertEquals([204] * 2, statusCodes)

    def testUpdateRecordWhileSendingData(self):
        batchSize = 3
        oaiJazz = OaiJazz(join(self.tempdir, 'oai'))
        storageComponent = MultiSequentialStorage(join(self.tempdir, 'storage'))
        self._addOaiRecords(storageComponent, oaiJazz, count=batchSize + 10)
        dna = be((Observable(),
            (OaiPmh(repositoryName='test', adminEmail='no@example.org', batchSize=batchSize),
                (storageComponent,),
                (oaiJazz,),
            )
        ))
        kwargs = dict(
            Method='GET',
            Headers={'Host': 'myserver'},
            port=1234,
            path='/oaipmh.pl',
            arguments=dict(verb=['ListIdentifiers'], metadataPrefix=['prefix']),
            )
        stream = compose(dna.all.handleRequest(**kwargs))
        buf = StringIO()
        for stuff in stream:
            buf.write(stuff)
            if 'identifier>id0<' in stuff:
                 oaiJazz.addOaiRecord(identifier="id1", sets=[], metadataFormats=[("prefix", "", "")])

        result = XML(buf.getvalue().split(CRLF*2)[-1])
        resumptionToken = xpathFirst(result, '/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken/text()')
        self.assertFalse(resumptionToken is None)


    def testNearRealtimeOaiSavesState(self):
        observer = CallTrace("observer", ignoredAttributes=["observer_init"], methods={'add': lambda **kwargs: (x for x in [])})
        oaiJazz = OaiJazz(join(self.tempdir, 'oai'))
        suspendRegister = SuspendRegister()
        oaiJazz.addObserver(suspendRegister)
        storageComponent = MultiSequentialStorage(join(self.tempdir, 'storage'))
        self._addOaiRecords(storageComponent, oaiJazz, 1)

        oaiPmhThread = None
        harvestThread = None

        def start():
            global oaiPmhThread, harvestThread
            self.run = True
            portNumber = randint(50000, 60000)
            oaiPmhThread = Thread(None, lambda: self.startOaiPmh(portNumber, oaiJazz, storageComponent, suspendRegister))
            harvestThread = Thread(None, lambda: self.startOaiHarvester(portNumber, observer))
            oaiPmhThread.start()
            harvestThread.start()

        def stop():
            global oaiPmhThread, harvestThread
            self.run = False
            oaiPmhThread.join()
            oaiPmhThread = None
            harvestThread.join()
            harvestThread = None

        start()
        requests = 1
        sleepWheel(1.0 + 1.0 * requests)
        self.assertEquals(['startOaiBatch', 'add', 'stopOaiBatch'], [m.name for m in observer.calledMethods])
        kwarg = lxmltostring(observer.calledMethods[1].kwargs['lxmlNode'])
        self.assertTrue("id0" in kwarg, kwarg)
        stop()
        observer.calledMethods.reset()

        storageComponent.addData(identifier="id1", name="prefix", data="<a>a1</a>")
        oaiJazz.addOaiRecord(identifier="id1", sets=[], metadataFormats=[("prefix", "", "")])

        start()
        requests = 1
        sleepWheel(1.0 + 1.0 * requests)
        self.assertEquals(['startOaiBatch', 'add', 'stopOaiBatch'], [m.name for m in observer.calledMethods])
        kwarg = lxmltostring(observer.calledMethods[1].kwargs['lxmlNode'])
        self.assertFalse("id0" in kwarg, kwarg)
        self.assertTrue("id1" in kwarg, kwarg)
        stop()

    def startOaiHarvester(self, portNumber, observer):
        reactor = Reactor()
        server = be(
            (Observable(),
                (PeriodicDownload(reactor, 'localhost', portNumber),
                    (XmlParseLxml(fromKwarg="data", toKwarg="lxmlNode"),
                        (OaiDownloadProcessor('/', 'prefix', self.tempdir),
                            (observer,),
                        )
                    )
                )
            )
        )
        list(compose(server.once.observer_init()))
        self._loopReactor(reactor)

    def startOaiPmh(self, portNumber, oaiJazz, storageComponent, register):
        getVMEnv().attachCurrentThread()
        reactor = Reactor()
        server = be(
            (Observable(),
                (ObservableHttpServer(reactor, portNumber),
                    (OaiPmh(repositoryName='repositoryName', adminEmail='adminEmail', batchSize=2, supportXWait=True),
                        (register,),
                        (oaiJazz,
                            (register,),
                        ),
                        (storageComponent,)
                    )
                )
            )
        )
        list(compose(server.once.observer_init()))
        self._loopReactor(reactor)

    def _addOaiRecords(self, storageComponent, oaiJazz, count):
        for i in range(count):
            storageComponent.addData(identifier="id%s" % i, name="prefix", data="<a>a%s</a>" % i)
            oaiJazz.addOaiRecord(identifier="id%s" % i, sets=[], metadataFormats=[("prefix", "", "")])

    def _loopReactor(self, reactor):
        def tick():
            reactor.addTimer(0.1, tick)
        tick()
        while self.run:
            reactor.step()


def xpath(node, path):
    return node.xpath(path, namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})


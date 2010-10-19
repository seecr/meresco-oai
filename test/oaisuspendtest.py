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

from os.path import join
from random import randint
from threading import Thread
from time import sleep

from meresco.core import be, Observable
from meresco.components.http import ObservableHttpServer
from meresco.components import StorageComponent
from meresco.oai import OaiPmh, OaiJazz, OaiSuspend, OaiHarvester

from cq2utils import CQ2TestCase, CallTrace
from weightless import Suspend, Reactor

from lxml.etree import tostring

class OaiSuspendTest(CQ2TestCase):

    def testAddSuspendedListRecord(self):
        oaiSuspend = OaiSuspend()
        suspend = oaiSuspend.suspend().next()
        self.assertTrue([suspend], oaiSuspend._suspended)
        self.assertEquals(Suspend, type(suspend))

    def testAddOaiRecord(self):
        oaiSuspend = OaiSuspend()
        observer = CallTrace("oaijazz")
        oaiSuspend.addObserver(observer)
        reactor = CallTrace("reactor")
        suspend = oaiSuspend.suspend().next()
        resumed = []
        suspend(reactor, lambda: resumed.append(True))

        oaiSuspend.addOaiRecord(identifier="ignored", sets="sets", metadataFormats="metadataFormats")

        self.assertEquals([True], resumed)
        self.assertEquals([], oaiSuspend._suspended)
        self.assertEquals("addOaiRecord", observer.calledMethods[0].name)

    def testDelete(self):
        oaiSuspend = OaiSuspend()
        observer = CallTrace("oaijazz")
        oaiSuspend.addObserver(observer)
        reactor = CallTrace("reactor")
        suspend = oaiSuspend.suspend().next()
        resumed = []
        suspend(reactor, lambda: resumed.append(True))

        oaiSuspend.delete(identifier='identifier')

        self.assertEquals([True], resumed)
        self.assertEquals([], oaiSuspend._suspended)
        self.assertEquals("delete", observer.calledMethods[0].name)

    def testNearRealtimeOai(self):
        self.run = True
        portNumber = randint(50000, 60000)
        observer = CallTrace("observer", ignoredAttributes=["observer_init"])
        oaiJazz = be(
            (OaiSuspend(),
                (OaiJazz(join(self.tempdir, 'oai')),),
            )
        )
        storageComponent = StorageComponent(join(self.tempdir, 'storage'))
        self._addOaiRecords(storageComponent, oaiJazz, 3)

        oaiPmhThread = Thread(None, lambda: self.startOaiPmh(portNumber, oaiJazz, storageComponent))
        harvestThread = Thread(None, lambda: self.startOaiHarvester(portNumber, observer))
        oaiPmhThread.start()
        harvestThread.start()

        requests = 3
        sleep(1.0 + 1.0 * requests)
       
        self.assertEquals(2, len(observer.calledMethods))
        kwarg = tostring(observer.calledMethods[0].kwargs['lxmlNode'])
        self.assertTrue("id0" in kwarg, kwarg)
        self.assertTrue("id1" in kwarg, kwarg)
        kwarg = tostring(observer.calledMethods[1].kwargs['lxmlNode'])
        self.assertTrue("id2" in kwarg, kwarg)

        self.assertEquals(1, len(oaiJazz._suspended))

        storageComponent.add("id3", "prefix", "<a>a3</a>")
        oaiJazz.addOaiRecord(identifier="id3", sets=[], metadataFormats=[("prefix", "", "")])
        sleep(0.1)
        self.assertEquals(0, len(oaiJazz._suspended))
        self.assertEquals(3, len(observer.calledMethods))
        kwarg = tostring(observer.calledMethods[2].kwargs['lxmlNode'])
        self.assertTrue("id3" in kwarg, kwarg)
        sleep(1.0)
        self.assertEquals(1, len(oaiJazz._suspended))

        self.run = False
        oaiPmhThread.join()
        harvestThread.join()

    def testNearRealtimeOaiSavesState(self):
        observer = CallTrace("observer", ignoredAttributes=["observer_init"])
        oaiJazz = be(
            (OaiSuspend(),
                (OaiJazz(join(self.tempdir, 'oai')),),
            )
        )
        storageComponent = StorageComponent(join(self.tempdir, 'storage'))
        self._addOaiRecords(storageComponent, oaiJazz, 1)
        
        oaiPmhThread = None
        harvestThread = None

        def start():
            global oaiPmhThread, harvestThread
            self.run = True
            portNumber = randint(50000, 60000)
            oaiPmhThread = Thread(None, lambda: self.startOaiPmh(portNumber, oaiJazz, storageComponent))
            harvestThread = Thread(None, lambda: self.startOaiHarvester(portNumber, observer))
            oaiPmhThread.start()
            harvestThread.start()

        def stop():
            self.run = False
            oaiPmhThread.join()
            harvestThread.join()

        start()
        requests = 1
        sleep(1.0 + 1.0 * requests)
        self.assertEquals(1, len(observer.calledMethods))
        kwarg = tostring(observer.calledMethods[0].kwargs['lxmlNode'])
        self.assertTrue("id0" in kwarg, kwarg)
        stop()

        storageComponent.add("id1", "prefix", "<a>a1</a>")
        oaiJazz.addOaiRecord(identifier="id1", sets=[], metadataFormats=[("prefix", "", "")])

        start()
        requests = 1
        sleep(1.0 + 1.0 * requests)
        self.assertEquals(2, len(observer.calledMethods))
        kwarg = tostring(observer.calledMethods[1].kwargs['lxmlNode'])
        self.assertFalse("id0" in kwarg, kwarg)
        self.assertTrue("id1" in kwarg, kwarg)
        stop()

    def startOaiHarvester(self, portNumber, observer):
        reactor = Reactor()
        server = be(
            (Observable(),
                (OaiHarvester(reactor, 'localhost', portNumber, '/', 'prefix', self.tempdir),
                    (observer,),
                )
            )
        )
        server.once.observer_init()
        self._loopReactor(reactor)

    def startOaiPmh(self, portNumber, oaiJazz, storageComponent):
        reactor = Reactor()
        server = be(
            (Observable(),
                (ObservableHttpServer(reactor, portNumber),
                    (OaiPmh(repositoryName='repositoryName', adminEmail='adminEmail', batchSize=2),
                        (oaiJazz,),
                        (storageComponent,)
                    )
                )
            )
        )
        server.once.observer_init()
        self._loopReactor(reactor)

    def _addOaiRecords(self, storageComponent, oaiJazz, count):
        for i in range(count):            
            storageComponent.add("id%s" % i, "prefix", "<a>a%s</a>" % i)
            oaiJazz.addOaiRecord(identifier="id%s" % i, sets=[], metadataFormats=[("prefix", "", "")])

    def _loopReactor(self, reactor):
        def tick():
            reactor.addTimer(0.1, tick)
        tick()
        while self.run:
            reactor.step()

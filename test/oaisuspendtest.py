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
        oaiSuspend.addOaiRecord("ignored", "sets", "metadataFormats")
        self.assertEquals([True], resumed)
        self.assertEquals([], oaiSuspend._suspended)
        self.assertEquals("addOaiRecord", observer.calledMethods[0].name)

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

        oaiPmhThread = Thread(None, lambda: self.startOaiPmh(portNumber, oaiJazz, storageComponent))
        harvestThread = Thread(None, lambda: self.startOaiHarvester(portNumber, observer))
        oaiPmhThread.start()
        harvestThread.start()

        requests = 3
        sleep(1.0 + 1.0 * requests)
       
        self.assertEquals(2, len(observer.calledMethods))
        arg = tostring(observer.calledMethods[0].args[0])
        self.assertTrue("id0" in arg, arg)
        self.assertTrue("id1" in arg, arg)
        arg = tostring(observer.calledMethods[1].args[0])
        self.assertTrue("id2" in arg, arg)

        self.assertEquals(1, len(oaiJazz._suspended))

        storageComponent.add("id3", "prefix", "<a>a3</a>")
        oaiJazz.addOaiRecord("id3", sets=[], metadataFormats=[("prefix", "", "")])
        sleep(0.1)
        self.assertEquals(0, len(oaiJazz._suspended))
        self.assertEquals(3, len(observer.calledMethods))
        arg = tostring(observer.calledMethods[2].args[0])
        self.assertTrue("id3" in arg, arg)
        sleep(1.0)
        self.assertEquals(1, len(oaiJazz._suspended))

        self.run = False
        oaiPmhThread.join()
        harvestThread.join()

    def startOaiHarvester(self, portNumber, observer):
        reactor = Reactor()
        server = be(
            (Observable(),
                (OaiHarvester(reactor, 'localhost', portNumber, '/', 'prefix'),
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

        for i in range(3):            
            storageComponent.add("id%s" % i, "prefix", "<a>a%s</a>" % i)
            oaiJazz.addOaiRecord("id%s" % i, sets=[], metadataFormats=[("prefix", "", "")])


        self._loopReactor(reactor)

    def _loopReactor(self, reactor):
        def tick():
            reactor.addTimer(0.1, tick)
        tick()
        while self.run:
            reactor.step()

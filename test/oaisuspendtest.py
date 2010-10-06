from cq2utils import CQ2TestCase, CallTrace
from meresco.oai import OaiSuspend
from weightless import Suspend

class OaiSuspendTest(CQ2TestCase):

    def testAddSuspendedListRecord(self):
        oaiSuspend = OaiSuspend()
        suspend = oaiSuspend.suspend().next()
        self.assertTrue([suspend], oaiSuspend._suspended)
        self.assertEquals(Suspend, type(suspend))

    def testResume(self):
        oaiSuspend = OaiSuspend()
        reactor = CallTrace("reactor")
        suspend = oaiSuspend.suspend().next()
        resumed = []
        suspend(reactor, lambda: resumed.append(True))
        oaiSuspend.resume()
        self.assertEquals([True], resumed)
        self.assertEquals([], oaiSuspend._suspended)

from cq2utils import CQ2TestCase, CallTrace
from meresco.oai import OaiSuspend

class OaiSuspendTest(CQ2TestCase):

    def testAddSuspendedListRecord(self):
        oaiSuspend = OaiSuspend()
        suspend = CallTrace("suspend")
        oaiSuspend.suspend(suspend)
        self.assertTrue([suspend], oaiSuspend._suspended)

    def testResume(self):
        oaiSuspend = OaiSuspend()
        suspend = CallTrace("suspend")
        oaiSuspend.suspend(suspend)
        oaiSuspend.resume()
        self.assertEquals("resume", suspend.calledMethods[0].name)
        self.assertEquals([], oaiSuspend._suspended)

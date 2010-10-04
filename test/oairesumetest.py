from cq2utils import CQ2TestCase, CallTrace
from meresco.oai import OaiResume

class OaiResumeTest(CQ2TestCase):

    def testAddSuspendedListRecord(self):
        oaiResume = OaiResume()
        suspend = CallTrace("suspend")
        oaiResume.addSuspend(suspend)
        self.assertTrue([suspend], oaiResume._suspended)

    def testCommit(self):
        oaiResume = OaiResume()
        suspend = CallTrace("suspend")
        oaiResume.addSuspend(suspend)
        oaiResume.commit()
        self.assertEquals("resume", suspend.calledMethods[0].name)
        self.assertEquals([], oaiResume._suspended)

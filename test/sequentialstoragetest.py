from seecr.test import SeecrTestCase
from meresco.oai import SequentialStorage

from os.path import join, isfile

class SequentialStorageTest(SeecrTestCase):

    def testA(self):
        s = SequentialStorage(self.tempdir)
        self.assertTrue(s)

    def testWriteFilePerPart(self):
        s = SequentialStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data/>")
        s.add("id02", "rdf", "<rdf/>")
        self.assertTrue(isfile(join(self.tempdir, "oai_dc")))
        self.assertTrue(isfile(join(self.tempdir, "rdf")))
  
    def testGetForUnknownPart(self):
        s = SequentialStorage(self.tempdir)
        self.assertRaises(KeyError, lambda: s.get('unknown', 'oai_dc'))

    def testGetForUnknownIdentifier(self):
        s = SequentialStorage(self.tempdir)
        s.add("id01", "oai_dc", "x")
        self.assertRaises(KeyError, lambda: s.get('unknown', 'oai_dc'))

    def testReadWriteData(self):
        s = SequentialStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data/>")
        sReopened = SequentialStorage(self.tempdir)
        self.assertEquals('<data/>', s.get('id01', 'oai_dc'))

    def testReadWriteIdentifier(self): 
        s = SequentialStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data>1</data>")
        s.add("id02", "oai_dc", "<data>2</data>")
        sReopened = SequentialStorage(self.tempdir)
        self.assertEquals('<data>1</data>', sReopened.get('id01', 'oai_dc'))
        self.assertEquals('<data>2</data>', sReopened.get('id02', 'oai_dc'))

    def testKeyIsMonotonicallyIncreasing(self):
        s = SequentialStorage(self.tempdir)
        s.add("3", "na", "na")
        s.add("4", "na", "na")
        try:
            s.add("2", "na", "na")
            self.fail()
        except ValueError, e:
            self.assertEquals("key 2 must be greater than last key 4", str(e))

    def testMonotonicityNotRequiredOverDifferentParts(self):
        s = SequentialStorage(self.tempdir)
        s.add("2", "oai_dc", "<data/>")
        s.add("2", "rdf", "<rdf/>")

    def testSentinalWritten(self):
        s = SequentialStorage(self.tempdir)
        s.add("3", "na", "na")
        self.assertEquals("--------\n3\nna\n", open(join(self.tempdir, 'na')).read())
    


from seecr.test import SeecrTestCase
from meresco.oai import SequentialStorage

from os.path import join, isfile

class SequentialStorageTest(SeecrTestCase):

    def testA(self):
        s = SequentialStorage(self.tempdir)
        self.assertTrue(s != None)

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
        self.assertEquals("-------\n3\nna\n", open(join(self.tempdir, 'na')).read())
    

    def testGetItem(self):
        # getitem need not be completely correct for bisect to work
        # the functionality below is good enough I suppose.
        # As a side effect, it solves back scanning! We let
        # bisect do that for us.
        s = SequentialStorage(self.tempdir)
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "oai_dc", "<data>two</data>")
        s.add("4", "oai_dc", "<data>four</data>")
        s.add("7", "oai_dc", "<data>seven</data>")
        self.assertEquals(10, len(s)) # artificial
        self.assertEquals(("2", "<data>two</data>"), s[0])
        self.assertEquals(("4", "<data>four</data>"), s[1])
        self.assertEquals(("4", "<data>four</data>"), s[2])
        self.assertEquals(("4", "<data>four</data>"), s[3])
        self.assertEquals(("7", "<data>seven</data>"), s[4])
        self.assertEquals(("7", "<data>seven</data>"), s[5])
        self.assertEquals(("7", "<data>seven</data>"), s[6])
        # hmm, we expect index 0-9 to work based on len()
        self.assertRaises(IndexError, lambda: s[7])

    def testIndexItem(self):
        s = SequentialStorage(self.tempdir)
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "oai_dc", "<data>two</data>")
        s.add("4", "oai_dc", "<data>four</data>")
        s.add("7", "oai_dc", "<data>seven</data>")
        s.index("4") 

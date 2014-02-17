from seecr.test import SeecrTestCase
from meresco.oai import SequentialStorage, SequentialMultiStorage

from os.path import join, isfile

class SequentialStorageTest(SeecrTestCase):

    def testA(self):
        s = SequentialMultiStorage(self.tempdir)
        self.assertTrue(s != None)

    def testWriteFilePerPart(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data/>")
        s.add("id02", "rdf", "<rdf/>")
        self.assertTrue(isfile(join(self.tempdir, "oai_dc")))
        self.assertTrue(isfile(join(self.tempdir, "rdf")))
  
    def testGetForUnknownPart(self):
        s = SequentialMultiStorage(self.tempdir)
        self.assertRaises(IndexError, lambda: s.get('unknown', 'oai_dc'))

    def testGetForUnknownIdentifier(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("id01", "oai_dc", "x")
        self.assertRaises(IndexError, lambda: s.get('unknown', 'oai_dc'))

    def testReadWriteData(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data/>")
        sReopened = SequentialMultiStorage(self.tempdir)
        self.assertEquals('<data/>', s.get('id01', 'oai_dc'))

    def testReadWriteIdentifier(self): 
        s = SequentialMultiStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data>1</data>")
        s.add("id02", "oai_dc", "<data>2</data>")
        sReopened = SequentialMultiStorage(self.tempdir)
        self.assertEquals('<data>1</data>', sReopened.get('id01', 'oai_dc'))
        self.assertEquals('<data>2</data>', sReopened.get('id02', 'oai_dc'))

    def testKeyIsMonotonicallyIncreasing(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("3", "na",  "na")
        s.add("4", "na",  "na")
        try:
            s.add("2", "na",  "na")
            self.fail()
        except ValueError, e:
            self.assertEquals("key 2 must be greater than last key 4", str(e))

    def testKeyIsMonotonicallyIncreasingAfterReload(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("3", "na",  "na")
        s = SequentialMultiStorage(self.tempdir)
        self.assertRaises(ValueError, lambda: s.add("2", "na", "na"))

    def testMonotonicityNotRequiredOverDifferentParts(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("2", "oai_dc", "<data/>")
        s.add("2", "rdf", "<rdf/>")

    def testSentinalWritten(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("3", "na", "data")
        self.assertEquals("----\n3\n4\ndata\n", open(join(self.tempdir, 'na')).read())

    def testGetItem(self):
        # getitem need not be completely correct for bisect to work
        # the functionality below is good enough I suppose.
        # As a side effect, it solves back scanning! We let
        # bisect do that for us.
        s = SequentialStorage(self.tempdir + "test")
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "<data>two</data>")
        s.add("4", "<data>four</data>")
        s.add("7", "<data>seven</data>")
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
        s = SequentialStorage(self.tempdir + "test")
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "<data>two</data>")
        s.add("4", "<data>four</data>")
        s.add("7", "<data>seven</data>")
        self.assertEquals(10, len(s)) # artificial
        self.assertEquals("<data>four</data>", s.index("4"))
        self.assertEquals("<data>two</data>", s.index("2"))
        self.assertEquals("<data>seven</data>", s.index("7"))

    def testIndexWithVerySmallAndVEryLargeRecord(self):
        s = SequentialStorage(self.tempdir + "test")
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "<data>short</data>")
        s.add("4", "<data>long</data>" * 1000)
        self.assertEquals(2130, len(s)) # artificial
        self.assertEquals("<data>short</data>", s.index("2"))
        self.assertEquals("<data>long</data><da", s.index("4")[:20])
       
    def testNewLineInData(self):
        s = SequentialStorage(self.tempdir + "test")
        s.add("4", "here follows\na new line")
        self.assertEquals("here follows\na new line", s.index("4"))
       
    def testSentinelInData(self):
        from meresco.oai.sequentialstorage import SENTINEL
        s = SequentialStorage(self.tempdir + "test")
        s.add("2", "<data>two</data>")
        s.add("5", ("abc%sxyz" % SENTINEL) * 10)
        s.add("7", "<data>seven</data>")
        s.add("9", "<data>nine</data>")
        self.assertEquals("abc----\nxyzabc----\nx", s.index("5")[:20])
        self.assertEquals("<data>seven</data>", s.index("7"))

    def testValidPartName(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("2", "ma/am", "data")
        s = SequentialMultiStorage(self.tempdir)
        self.assertEquals("data", s.get("2", "ma/am"))

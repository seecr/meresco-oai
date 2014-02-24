## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from seecr.test import SeecrTestCase
from meresco.oai import SequentialStorage, SequentialMultiStorage

from os.path import join, isfile, isdir

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
        self.assertRaises(IndexError, lambda: s.getData('unknown', 'oai_dc'))

    def testGetForUnknownIdentifier(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("id01", "oai_dc", "x")
        self.assertRaises(IndexError, lambda: s.getData('unknown', 'oai_dc'))

    def testReadWriteData(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data/>")
        s.flush()
        sReopened = SequentialMultiStorage(self.tempdir)
        self.assertEquals('<data/>', s.getData('id01', 'oai_dc'))

    def testReadWriteIdentifier(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("id01", "oai_dc", "<data>1</data>")
        s.add("id02", "oai_dc", "<data>2</data>")
        s.flush()
        sReopened = SequentialMultiStorage(self.tempdir)
        self.assertEquals('<data>1</data>', sReopened.getData('id01', 'oai_dc'))
        self.assertEquals('<data>2</data>', sReopened.getData('id02', 'oai_dc'))

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
        s.flush()
        s = SequentialMultiStorage(self.tempdir)
        self.assertRaises(ValueError, lambda: s.add("2", "na", "na"))

    def testMonotonicityNotRequiredOverDifferentParts(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("2", "oai_dc", "<data/>")
        s.add("2", "rdf", "<rdf/>")

    def testNumericalKeys(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add(2, "oai_dc", "<two/>")
        s.add(4, "oai_dc", "<four/>")
        s.add(7, "oai_dc", "<seven/>")
        self.assertEquals([('2', '<two/>'), ('4', '<four/>')], list(s.iterData("oai_dc", 1, 5)))
        self.assertEquals([('7', '<seven/>')], list(s.iterData("oai_dc", 5, 9)))
        self.assertEquals("<two/>", s.getData(2, "oai_dc"))

    def testSentinalWritten(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("3", "na", "data")
        s.flush()
        self.assertEquals("----\n3\n12\nx\x9cKI,I\x04\x00\x04\x00\x01\x9b\n",
                open(join(self.tempdir, 'na')).read())

    def testGetItem(self):
        # getitem need not be completely correct for bisect to work
        # the functionality below is good enough I suppose.
        # As a side effect, it solves back scanning! We let
        # bisect do that for us.
        s = SequentialStorage(self.tempdir + "test")
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "<data>two is nice</data>")
        s.add("4", "<data>four goes fine</data>")
        s.add("7", "<data>seven seems ok</data>")
        self.assertEquals(11, len(s)) # artificial
        self.assertEquals(("2", "<data>two is nice</data>"), s[0])
        self.assertEquals(("4", "<data>four goes fine</data>"), s[1])
        self.assertEquals(("4", "<data>four goes fine</data>"), s[2])
        self.assertEquals(("4", "<data>four goes fine</data>"), s[3])
        self.assertEquals(("7", "<data>seven seems ok</data>"), s[4])
        self.assertEquals(("7", "<data>seven seems ok</data>"), s[5])
        self.assertEquals(("7", "<data>seven seems ok</data>"), s[6])
        self.assertEquals(("7", "<data>seven seems ok</data>"), s[7])
        # hmm, we expect index 0-10 to work based on len()
        self.assertRaises(IndexError, lambda: s[8])

    def testIndexItem(self):
        s = SequentialStorage(self.tempdir + "test")
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "<data>two</data>")
        s.add("4", "<data>four</data>")
        s.add("7", "<data>seven</data>")
        self.assertEquals(8, len(s)) # artificial
        self.assertEquals("<data>four</data>", s.index("4"))
        self.assertEquals("<data>two</data>", s.index("2"))
        self.assertEquals("<data>seven</data>", s.index("7"))

    def testIndexNotFound(self):
        s = SequentialStorage(self.tempdir + "test")
        self.assertRaises(IndexError, lambda: s.index("2"))
        s.add("2", "<data>two</data>")
        self.assertRaises(IndexError, lambda: s.index("1"))
        self.assertRaises(IndexError, lambda: s.index("3"))
        s.add("4", "<data>four</data>")
        self.assertRaises(IndexError, lambda: s.index("1"))
        self.assertRaises(IndexError, lambda: s.index("3"))
        self.assertRaises(IndexError, lambda: s.index("5"))

    def testIndexWithVerySmallAndVEryLargeRecord(self):
        s = SequentialStorage(self.tempdir + "test")
        self.assertEquals(0, len(s)) # artificial
        s.add("2", "<data>short</data>")
        s.add("4", ''.join("<%s>" % i for i in xrange(10000)))
        self.assertEquals(2011, len(s)) # artificial
        self.assertEquals("<data>short</data>", s.index("2"))
        self.assertEquals("<0><1><2><3><4><5><6", s.index("4")[:20])

    def testNewLineInData(self):
        s = SequentialStorage(self.tempdir + "test")
        s.add("4", "here follows\na new line")
        self.assertEquals("here follows\na new line", s.index("4"))

    def testSentinelInData(self):
        from meresco.oai.sequentialstorage import SENTINEL
        s = SequentialStorage(self.tempdir + "test")
        s.add("2", "<data>two</data>")
        s.add("5", ("abc%sxyz" % (SENTINEL+'\n')) * 10)
        s.add("7", "<data>seven</data>")
        s.add("9", "<data>nine</data>")
        self.assertEquals("abc----\nxyzabc----\nx", s.index("5")[:20])
        self.assertEquals("<data>seven</data>", s.index("7"))

    def testValidPartName(self):
        s = SequentialMultiStorage(self.tempdir)
        s.add("2", "ma/am", "data")
        s.flush()
        s = SequentialMultiStorage(self.tempdir)
        self.assertEquals("data", s.getData("2", "ma/am"))

    def testCompression(self):
        import zlib, bz2
        def ratio(filename, compress):
            data = open(filename).read()
            compressed = compress(data)
            return len(data)/float(len(compressed))
        zlib_ratio = ratio('trijntje.ggc.xml', zlib.compress)
        bz2_ratio = ratio('trijntje.ggc.xml', bz2.compress)
        self.assertTrue(3.0 < bz2_ratio < 3.1, bz2_ratio)
        self.assertTrue(3.4 < zlib_ratio < 3.5, zlib_ratio)
        zlib_ratio = ratio('trijntje.xml', zlib.compress)
        bz2_ratio = ratio('trijntje.xml', bz2.compress)
        self.assertTrue(2.5 < bz2_ratio < 2.6, bz2_ratio)
        self.assertTrue(3.2 < zlib_ratio < 3.3, zlib_ratio)

    def testIterator(self):
        s = SequentialStorage(self.tempdir + "test")
        s.add("2", "<data>two</data>")
        s.add("4", "<data>four</data>")
        s.add("7", "<data>seven</data>")
        s.add("9", "<data>nine</data>")
        i = s.iter("3")
        self.assertEquals(('4', "<data>four</data>"), i.next())
        self.assertEquals(('7', "<data>seven</data>"), i.next())
        self.assertEquals(('9', "<data>nine</data>"), i.next())
        self.assertRaises(StopIteration, lambda: i.next())

    def testTwoAlternatingIterators(self):
        s = SequentialStorage(self.tempdir + "test")
        s.add("2", "<data>two</data>")
        s.add("4", "<data>four</data>")
        s.add("7", "<data>seven</data>")
        s.add("9", "<data>nine</data>")
        i1 = s.iter("4")
        i2 = s.iter("7")
        self.assertEquals(("7", "<data>seven</data>"), i2.next())
        self.assertEquals(("4", "<data>four</data>"), i1.next())
        self.assertEquals(("9", "<data>nine</data>"), i2.next())
        self.assertEquals(("7", "<data>seven</data>"), i1.next())
        self.assertRaises(StopIteration, lambda: i2.next())
        self.assertEquals(("9", "<data>nine</data>"), i1.next())
        self.assertRaises(StopIteration, lambda: i1.next())

    def testIteratorUntil(self):
        s = SequentialStorage(self.tempdir + "test")
        s.add("2", "two")
        s.add("4", "four")
        s.add("7", "seven")
        s.add("9", "nine")
        i = s.iter("0", "5")
        self.assertEquals([('2', "two"), ('4', "four")], list(i))
        i = s.iter("4", "7") #inclusive????
        self.assertEquals([('4', "four"), ('7', "seven")], list(i))
        i = s.iter("5", "99")
        self.assertEquals([('7', "seven"), ('9', "nine")], list(i))

    def testReadSpeed(self):
        from random import random, randint
        from time import time
        count = 1000000
        s = SequentialStorage(self.tempdir + '/test')
        data = ''.join(str(random()) for f in xrange(300))
        self.assertTrue(4000 < len(data) < 5000, len(data))
        bytesWritten = 0
        t0 = time()
        for i in xrange(count, count+count):
            bytesWritten += len(data)
            s.add(str(i), data)
            if i % 1000 == 0:
                t1 = time()
                recordsPerSecond = (i-count)/(t1-t0)
                bytesPerSecond = bytesWritten/(t1-t0)
                print bytesWritten, recordsPerSecond, bytesPerSecond
                if bytesWritten > 2*10**9: break
        n = 0
        t0 = time()
        print count, i
        for j in xrange(10000):
            data = s.index(str(randint(count, i)))
            n += 1
            t1 = time()
            if j % 1000 == 0:
                lookupsPerSecond = n / (t1-t0)
                print lookupsPerSecond
        print "lookups:", n, "time:", (t1-t0), "lookups/second:", n/(t1-t0)

    def testDirectoryCreatedIfNotExists(self):
        SequentialMultiStorage(join(self.tempdir, "storage"))
        self.assertTrue(isdir(join(self.tempdir, "storage")))

    def testReadOnlyKeyWhileSearching(self):
        pass

    # _index kan weg
    # flush -> handle shutdown


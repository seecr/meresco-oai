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

from os.path import join, isfile, isdir
from seecr.test import SeecrTestCase
from weightless.core import consume
from meresco.oai import SequentialStorage, SequentialMultiStorage
from meresco.oai.sequentialstorage import SENTINEL

class SequentialStorageTest(SeecrTestCase):

    def testWriteFilePerPart(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "<data/>"))
        consume(s.add(2, "rdf", "<rdf/>"))
        self.assertTrue(isfile(join(self.tempdir, "oai_dc")))
        self.assertTrue(isfile(join(self.tempdir, "rdf")))

    def testGetForUnknownPart(self):
        s = SequentialMultiStorage(self.tempdir)
        self.assertRaises(IndexError, lambda: s.getData(42, 'oai_dc'))

    def testGetForUnknownIdentifier(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "x"))
        self.assertRaises(IndexError, lambda: s.getData(42, 'oai_dc'))

    def testReadWriteData(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "<data/>"))
        s.flush()
        sReopened = SequentialMultiStorage(self.tempdir)
        self.assertEquals('<data/>', sReopened.getData(1, 'oai_dc'))

    def testReadWriteIdentifier(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "<data>1</data>"))
        consume(s.add(2, "oai_dc", "<data>2</data>"))
        s.flush()
        sReopened = SequentialMultiStorage(self.tempdir)
        self.assertEquals('<data>1</data>', sReopened.getData(1, 'oai_dc'))
        self.assertEquals('<data>2</data>', sReopened.getData(2, 'oai_dc'))

    def testKeyIsMonotonicallyIncreasing(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(3, "na",  "na"))
        consume(s.add(4, "na",  "na"))
        try:
            consume(s.add(2, "na",  "na"))
            self.fail()
        except ValueError, e:
            self.assertEquals("key 2 must be greater than last key 4", str(e))

    def testNumbersAsStringIsProhibited(self):
        s = SequentialStorage(self.tempfile)
        s.add(2, "na")
        s.add(10, "na")
        self.assertRaises(ValueError, lambda: s.add('3', "na"))

    def XXXtestKeyIsMonotonicallyIncreasingAfterReload(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(3, "na",  "na"))
        s.flush()
        s = SequentialMultiStorage(self.tempdir)
        self.assertRaises(ValueError, lambda: consume(s.add(2, "na", "na")))

    def XXXtestDataCanBeEmptyButStoredItemIsNeverShorterThanBlocksize(self):
        s = SequentialStorage(self.tempfile)
        s.add(0, '')
        s.flush()
        fileData = open(self.tempfile, 'rb').read()
        self.assertTrue(len(fileData) >= BLOCKSIZE, len(fileData))

        # whitebox, blocksize 'mocked data' is 1-byte
        from zlib import compress
        self.assertEquals(18, len(fileData))
        self.assertEquals(BLOCKSIZE - 1 + len(compress('')), len(fileData))

    def XXXtestLastKeyFoundInCaseOfLargeBlock(self):
        s = SequentialStorage(self.tempfile)
        s.add(1, 'record 1')
        s.add(2, 'long record' * 1000) # compressed multiple times BLOCKSIZE
        s.flush()
        s = SequentialStorage(self.tempfile)
        self.assertEquals(2, s._lastKey)

    def testMonotonicityNotRequiredOverDifferentParts(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(2, "oai_dc", "<data/>"))
        consume(s.add(2, "rdf", "<rdf/>"))

    def testNumericalKeys(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(2, "oai_dc", "<two/>"))
        consume(s.add(4, "oai_dc", "<four/>"))
        consume(s.add(7, "oai_dc", "<seven/>"))
        self.assertEquals([(2, '<two/>'), (4, '<four/>')], list(s.iterData("oai_dc", 1, 5)))
        self.assertEquals([(7, '<seven/>')], list(s.iterData("oai_dc", 5, 9)))
        self.assertEquals("<two/>", s.getData(2, "oai_dc"))

    def testSentinalWritten(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(3, "na", "data"))
        s.flush()
        self.assertEquals("----\n3\n12\nx\x9cKI,I\x04\x00\x04\x00\x01\x9b\n",
                open(join(self.tempdir, 'na')).read())

    def testGetItem(self):
        # getitem need not be completely correct for bisect to work
        # the functionality below is good enough I suppose.
        # As a side effect, it solves back scanning! We let
        # bisect do that for us.
        s = SequentialStorage(self.tempfile, blkSize=11)
        self.assertEquals(0, len(s._blkIndex))
        s.add(2, "<data>two is nice</data>")
        s.add(4, "<data>four goes fine</data>")
        s.add(7, "<data>seven seems ok</data>")
        self.assertEquals(11, len(s._blkIndex))
        self.assertEquals((2, "<data>two is nice</data>"), s._blkIndex.scan(0))
        self.assertEquals((4, "<data>four goes fine</data>"), s._blkIndex.scan(1))
        self.assertEquals((4, "<data>four goes fine</data>"), s._blkIndex.scan(2))
        self.assertEquals((4, "<data>four goes fine</data>"), s._blkIndex.scan(3))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(4))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(5))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(6))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(7))
        # hmm, we expect index 0-10 to work based on len()
        self.assertRaises(StopIteration, lambda: s._blkIndex.scan(8))

    def testIndexItem(self):
        s = SequentialStorage(self.tempfile, blkSize=11)
        self.assertEquals(0, len(s._blkIndex))
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        self.assertEquals(8, len(s._blkIndex))
        self.assertEquals("<data>four</data>", s[4])
        self.assertEquals("<data>two</data>", s[2])
        self.assertEquals("<data>seven</data>", s[7])

    def testIndexNotFound(self):
        s = SequentialStorage(self.tempfile)
        self.assertRaises(IndexError, lambda: s[2])
        s.add(2, "<data>two</data>")
        self.assertRaises(IndexError, lambda: s[1])
        self.assertRaises(IndexError, lambda: s[3])
        s.add(4, "<data>four</data>")
        self.assertRaises(IndexError, lambda: s[1])
        self.assertRaises(IndexError, lambda: s[3])
        self.assertRaises(IndexError, lambda: s[5])

    def testIndexWithVerySmallAndVEryLargeRecord(self):
        s = SequentialStorage(self.tempfile, blkSize=11)
        self.assertEquals(0, len(s._blkIndex))
        s.add(2, "<data>short</data>")
        s.add(4, ''.join("<%s>" % i for i in xrange(10000)))
        self.assertEquals(2011, len(s._blkIndex))
        self.assertEquals("<data>short</data>", s[2])
        self.assertEquals("<0><1><2><3><4><5><6", s[4][:20])

    def testNewLineInData(self):
        s = SequentialStorage(self.tempfile)
        s.add(4, "here follows\na new line")
        self.assertEquals("here follows\na new line", s[4])

    def testSentinelInData(self):
        from meresco.oai.sequentialstorage import SENTINEL
        s = SequentialStorage(self.tempfile)
        s.add(2, "<data>two</data>")
        s.add(5, ("abc%sxyz" % (SENTINEL+'\n')) * 10)
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        self.assertEquals("abc----\nxyzabc----\nx", s[5][:20])
        self.assertEquals("<data>seven</data>", s[7])

    def XXXtestValidPartName(self):
        s = SequentialMultiStorage(self.tempdir)
        consume(s.add(2, "ma/am", "data"))
        s.flush()
        s = SequentialMultiStorage(self.tempdir)
        self.assertEquals("data", s.getData(2, "ma/am"))

    def XXXtestReadNextWithTargetKey(self):
        s = SequentialStorage(self.tempfile)
        s.add(3, "three")
        s.add(4, "four")
        s.add(7, "seven")
        s.add(9, "nine")
        s._f.seek(0 * BLOCKSIZE)
        self.assertRaises(StopIteration, lambda: s._readNext(target_key=2))
        self.assertRaises(StopIteration, lambda: s._readNext(target_key=5))
        s._f.seek(0 * BLOCKSIZE)
        self.assertEquals("three", s._readNext(target_key=3)[1])
        self.assertEquals("four", s._readNext(target_key=4)[1])
        s._f.seek(0 * BLOCKSIZE)
        self.assertEquals("four", s._readNext(target_key=4)[1])
        self.assertEquals("seven", s._readNext(target_key=7)[1])
        s._f.seek(0 * BLOCKSIZE)
        self.assertEquals("nine", s._readNext(target_key=9)[1])
        try:
            s._readNext(target_key=3)
            self.fail()
        except StopIteration:
            pass

    def XXXtestTargetKeyGreaterOrEquals(self):
        s = SequentialStorage(self.tempfile)
        s.add(3, "three")
        s.add(7, "seven")
        s.add(9, "nine")
        s._f.seek(0 * BLOCKSIZE)
        self.assertEquals("three", s._readNext(target_key=2, greater=True)[1])
        self.assertEquals("seven", s._readNext(target_key=7, greater=True)[1])
        self.assertEquals("nine", s._readNext(target_key=8, greater=True)[1])

    def XXXtestCompression(self):
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

    def XXXtestIterator(self):
        s = SequentialStorage(self.tempfile)
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        i = s.iter(3)
        self.assertEquals((4, "<data>four</data>"), i.next())
        self.assertEquals((7, "<data>seven</data>"), i.next())
        self.assertEquals((9, "<data>nine</data>"), i.next())
        self.assertRaises(StopIteration, lambda: i.next())

    def XXXtestTwoAlternatingIterators(self):
        s = SequentialStorage(self.tempfile)
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        i1 = s.iter(4)
        i2 = s.iter(7)
        self.assertEquals((7, "<data>seven</data>"), i2.next())
        self.assertEquals((4, "<data>four</data>"), i1.next())
        self.assertEquals((9, "<data>nine</data>"), i2.next())
        self.assertEquals((7, "<data>seven</data>"), i1.next())
        self.assertRaises(StopIteration, lambda: i2.next())
        self.assertEquals((9, "<data>nine</data>"), i1.next())
        self.assertRaises(StopIteration, lambda: i1.next())

    def XXXtestIteratorUntil(self):
        s = SequentialStorage(self.tempfile)
        s.add(2, "two")
        s.add(4, "four")
        s.add(6, "six")
        s.add(7, "seven")
        s.add(8, "eight")
        s.add(9, "nine")
        i = s.iter(0, 5)
        self.assertEquals([(2, "two"), (4, "four")], list(i))
        i = s.iter(4, 7)
        self.assertEquals([(4, "four"), (6, "six")], list(i))
        i = s.iter(4, 7, inclusive=True)
        self.assertEquals([(4, "four"), (6, "six"), (7, 'seven')], list(i))
        i = s.iter(5, 99)
        self.assertEquals([(6, "six"), (7, "seven"), (8, "eight"), (9, "nine")], list(i))

    def XXXtestIterDataUntil(self):
        s = SequentialMultiStorage(self.tempdir)
        s.addData(name='oai_dc', key=2, data="two")
        s.addData(name='oai_dc', key=4, data="four")
        s.addData(name='oai_dc', key=6, data="six")
        s.addData(name='oai_dc', key=7, data="seven")
        s.addData(name='oai_dc', key=8, data="eight")
        s.addData(name='oai_dc', key=9, data="nine")
        i = s.iterData(name='oai_dc', start=0, stop=5)
        self.assertEquals([(2, "two"), (4, "four")], list(i))
        i = s.iterData(name='oai_dc', start=4, stop=7)
        self.assertEquals([(4, "four"), (6, "six")], list(i))
        i = s.iterData(name='oai_dc', start=4, stop=7, inclusive=True)
        self.assertEquals([(4, "four"), (6, "six"), (7, 'seven')], list(i))
        i = s.iterData(name='oai_dc', start=5, stop=99)
        self.assertEquals([(6, "six"), (7, "seven"), (8, "eight"), (9, "nine")], list(i))

    def createTestIndex(self):
        from random import random, randint
        count = 2**20
        s = SequentialStorage("data/test.ss")
        if s.isEmpty():
            data = ''.join(str(random()) for f in xrange(300))
            for i in xrange(count):
                s.add(i, data)
                if i % 10000 == 0:
                    print i
        del s
        return SequentialStorage("data/test.ss")

    def XXXtestBigBlock(self):
        b = self.createTestIndex()._bigblks
        self.assertEquals(272504, len(b))
        self.assertEquals(0, b[0])
        self.assertEquals(4, b[1])
        self.assertEquals(3017, b[783])
        self.assertEquals(3017, b.scan(783, target_key=3017)[0])
        self.assertEquals(3018, b.scan(783, target_key=3018)[0])
        self.assertEquals(3019, b.scan(783, target_key=3019)[0])
        self.assertEquals(3020, b.scan(783, target_key=3020)[0])
        self.assertEquals(3021, b.scan(783, target_key=3021)[0])
        self.assertEquals(3021, b[784])
        self.assertEquals(1048571, b[272504-1])
        blk = b.search(864123)
        self.assertEquals(224561, blk)
        self.assertEquals(864122, b[blk])
        self.assertEquals(864126, b[blk+1])
        self.assertEquals(864123, b.scan(blk, target_key=864123)[0])

    def XXXtestGapsInMemIndex(self):
        count = 2**20
        from random import random, randint
        s = SequentialStorage("data/test.ss", cutoff=2048)
        for j in xrange(10000):
            s[randint(0, count-1)]
        last = None
        for i in s._index._memIndex._blks:
            if last:
                self.assertTrue(i > last + 512, (last, i, i-last))
            last = i

    def XXXtestReadSpeed(self):
        self.createTestIndex()
        from random import random, randint
        from time import time
        def f(cutoff=2**13):
            s = SequentialStorage("data/test.ss", cutoff=cutoff)
            n = 0
            t0 = time()
            for j in xrange(200000):
                i = randint(0, count-1)
                data = s[i]
                n += 1
            t1 = time()
            lookupsPerSecond = n / (t1-t0)
            c = s._index._memIndex._keys
            print cutoff, int(lookupsPerSecond), len(c)
        from seecr.utils.profileit import profile
        #profile(f)
        for cutoff in [1024, 2048, 4096, 2**13]:
            f(cutoff)

    def test64BitsArchRequiredForArrayL(self):
        self.fail('think')

    def testArrayPerformance(self):
        self.fail('not now')
        from random import randint
        from array import array
        from bisect import insort
        from time import time
        from sys import getsizeof
        a = array('L')
        #a = []
        self.assertEquals(0, len(a))
        self.assertEquals([], list(a))
        t0 = time()
        for i in xrange(10**7):
            insort(a, randint(0, 9999999999))
            t1 = time()
            if i % 10000 == 1:
                print (t1-t0), i, (t1-t0)/i, getsizeof(a), getsizeof(a)/i
        # list: 161.974352837 999999 0.000161974514811 8697472 8
        # array: 107.27125597 999999 0.000107271363241 56 0

    def XXXtestMemIndexSortes(self):
        mi = _MemIndex()
        mi.add(42, 88).add(40, 78).add(44, 98)
        self.assertEquals(sorted(mi._keys), [i for i in mi._keys])
        self.assertEquals(sorted(mi._blks), [i for i in mi._blks])

    def XXXtestMemIndexIgnoresDuplicates(self):
        mi = _MemIndex()
        mi.add(42, 88).add(40, 78).add(42, 98)
        self.assertEquals(2, len(mi))
   
    def XXXtestMemIndex(self):
        mi = _MemIndex()
        mi.add(40, 78).add(42, 88).add(44, 98)
        self.assertEquals(( 0, 78), mi.find_blk_range(39))
        self.assertEquals(( 0, 78), mi.find_blk_range(40))
        self.assertEquals((78, 88), mi.find_blk_range(41))
        self.assertEquals((78, 88), mi.find_blk_range(42))
        self.assertEquals((88, 98), mi.find_blk_range(43))
        self.assertEquals((88, 98), mi.find_blk_range(44))
        self.assertEquals((98, None), mi.find_blk_range(45))

    def XXXtestTwoLevelIndex(self):
        src = [3, 5, 6, 7, 9]
        index = _KeyIndex(src, "NA")
        n = index[0]
        self.assertEquals(3, n)

    def XXXtestDirectoryCreatedIfNotExists(self):
        SequentialMultiStorage(join(self.tempdir, "storage"))
        self.assertTrue(isdir(join(self.tempdir, "storage")))

    def XXXtestShortRubbishAtStartOfFileIgnored(self):
        s = ReopeningSeqStorage(self).write('corrupt')
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith("corrupt" + SENTINEL + '\n1\n'), s.fileData)

    def XXXtestLongerRubbishAtStartOfFileIgnored(self):
        s = ReopeningSeqStorage(self).write('corrupt' * 3)  # > BLOCKSIZE
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith("corrupt" * 3 + SENTINEL + '\n1\n'), s.fileData)

    def XXXtestCorruptionFromKeyLineIgnored(self):
        s = ReopeningSeqStorage(self).write('%s\ncorrupt' % SENTINEL)
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith(SENTINEL + "\ncorrupt" + SENTINEL + '\n1\n'), s.fileData)

    def XXXtestCorruptionFromLengthLineIgnored(self):
        s = ReopeningSeqStorage(self).write('%s\n1\ncorrupt' % SENTINEL)
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith(SENTINEL + "\n1\ncorrupt" + SENTINEL + '\n1\n'), s.fileData)

    def XXXtestCorruptionFromDataIgnored(self):
        s = ReopeningSeqStorage(self).write('%s\n1\n100\ncorrupt' % SENTINEL)
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith(SENTINEL + "\n1\n100\ncorrupt" + SENTINEL + '\n1\n'), s.fileData)

    def XXXtestRubbishInBetween(self):
        s = ReopeningSeqStorage(self)
        s.add(1, "record 1")
        s.write("rubbish")
        s.add(2, "record 2")
        self.assertEquals([(1, 'record 1'), (2, 'record 2')], s.items())

    def XXXtestCorruptionInBetween(self):
        s = ReopeningSeqStorage(self)
        s.add(5, "record to be corrupted")
        corruptRecordTemplate = s.fileData

        def _writeRecordAndPartOfRecord(i):
            open(self.tempfile, 'w').truncate(0)
            s.add(1, "record 1")
            s.write(corruptRecordTemplate[:i+1])

        for i in xrange(len(corruptRecordTemplate) - 2):
            _writeRecordAndPartOfRecord(i)
            s.add(2, "record 2")
            self.assertEquals([1, 2], s.keys())

        for i in xrange(len(corruptRecordTemplate) - 2, len(corruptRecordTemplate)):
            _writeRecordAndPartOfRecord(i)
            self.assertEquals([1, 5], s.keys())

    def XXXtestTargetKeySkipsRubbish(self):
        s = SequentialStorage(self.tempfile)
        s.add(5, "five")
        s._f.write("@!^$#%")
        s.add(6, "six")
        s._f.seek(0)
        self.assertEquals("six", s._readNext(6)[1])

    def XXXtestTargetKeyDoesNotSkipRecordWhenRubbishPresent(self):
        s = SequentialStorage(self.tempfile)
        s.add(5, "five")
        s._f.seek(-2, 1)
        s._f.write("@!^$#%")
        s.add(6, "six")
        s._f.seek(0)
        self.assertEquals("six", s._readNext(6)[1])

    def XXXtestTargetKeyDoesNotSkipRecordThatHappensToBeTruncated(self):
        s = SequentialStorage(self.tempfile)
        s.add(5, "five")
        s._f.truncate(s._f.tell()-2)  # 5 becomes crap, but six is still ok
        s.add(6, "six")
        s._f.seek(0)
        try:
            s._readNext(5)
            self.fail()
        except StopIteration:
            pass
        s._f.seek(0) #TODO what if this seek is not there? What should the file position be?
        self.assertEquals("six", s._readNext(6)[1])

        
class ReopeningSeqStorage(object):
    def __init__(self, testCase):
        self.tempfile = testCase.tempfile

    def add(self, key, data):
        s = SequentialStorage(self.tempfile)
        s.add(key, data)
        s.flush()
        return self

    def keys(self):
        return [item[0] for item in self.items()]

    def items(self):
        s = SequentialStorage(self.tempfile)
        return list(s.iter(0))

    def write(self, rubbish):
        with open(self.tempfile, 'ab') as f:
            f.write(rubbish)
        return self

    @property
    def fileData(self):
        return open(self.tempfile).read()

    def seqStorage(self):
        return SequentialStorage(self.tempfile)


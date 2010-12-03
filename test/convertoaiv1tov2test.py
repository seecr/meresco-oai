from os import system
from os.path import dirname, join
from shutil import copytree

from cq2utils import CQ2TestCase
from meresco.components import PersistentSortedIntegerList
from meresco.components.facetindex import IntegerList

class ConvertOaiV1ToV2Test(CQ2TestCase):
    def testConversion(self):
        thisdir = dirname(__file__)
        datadir = join(self.tempdir, 'oai_conversion_v1_to_v2')
        copytree(join(thisdir, 'data', 'oai_conversion_v1_to_v2'), datadir)

        anotherSet = IntegerList(10, use64bits=True)
        anotherSet.save(join(datadir, 'sets', 'anotherSet.list'), offset=0, append=False)
        anotherSetDeleted = IntegerList(5, use64bits=True)
        anotherSetDeleted.save(join(datadir, 'sets', 'anotherSet.list.deleted'), offset=0, append=False)

        system("%s %s" % (join(thisdir, '..', 'bin', 'convert_oai_v1_to_v2.py'), join(self.tempdir, 'oai_conversion_v1_to_v2')))

        expectedList = PersistentSortedIntegerList(join(self.tempdir, 'forAssertEquals'), use64bits=True)
        for i in xrange(10 ** 3):
            expectedList.append(i)
        expectedList.remove(200)
        expectedList.remove(600)
        expectedList.remove(4)
        for listName in ['tombStones', 'prefixes/somePrefix', 'sets/someSet']:
            converted = PersistentSortedIntegerList(join(datadir, listName + '.list'), use64bits=True)
            self.assertEquals(list(expectedList), list(converted))

        convertedAnotherSet = PersistentSortedIntegerList(join(datadir, 'sets', 'anotherSet.list'), use64bits=True)
        self.assertEquals(range(5, 10), list(convertedAnotherSet))

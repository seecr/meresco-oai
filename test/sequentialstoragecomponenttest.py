from seecr.test import SeecrTestCase
from meresco.oai import SequentialStorageComponent

from weightless.core import consume

class SequentialStorageComponentTest(SeecrTestCase):

    def testOne(self):
        c = SequentialStorageComponent(self.tempdir)
        self.assertEquals((False, False), c.isAvailable("an arbitratry identifier", partname="xml"))
        consume(c.add(identifier="an arbitratry identifier", partname="xml", data="<data/>"))
        self.assertTrue(c.isAvailable("an arbitratry identifier", partname="xml"))
        dataStream = c.getStream("an arbitratry identifier", "xml")
        self.assertEquals("<data/>", dataStream.read())

    def testTwo(self):
        c = SequentialStorageComponent(self.tempdir)
        consume(c.add("uri-1", "xml", "<xml1/>"))
        consume(c.add("uri-2", "xml", "<xml2/>"))
        self.assertEquals("<xml1/>", c.getStream("uri-1", "xml").read())
        self.assertEquals("<xml2/>", c.getStream("uri-2", "xml").read())

    def testSpeed(self):
        from time import time
        from random import randint
        N = 2500
        c = SequentialStorageComponent(self.tempdir)
        H = "This is an holding like records, at least, it tries to look like it, but I am not sure if it is really something that comes close enough.  Anyway, here you go: Holding: %s"
        self.assertEquals(171, len(H))
        t0 = time()
        for i in xrange(N):
            consume(c.add("http://nederland.nl/%s" % i, "xml", H))
            j = randint(0, i)
            data = c.getStream("http://nederland.nl/%s" % j, "xml").read()
            #self.assertEquals(H % j, data)
            if i % 1000 == 0:
                t1 = time()
                print i, i/(t1-t0)

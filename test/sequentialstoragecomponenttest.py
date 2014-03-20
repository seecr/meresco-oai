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
        N = 100000
        c = SequentialStorageComponent(self.tempdir)
        t0 = time()
        for i in xrange(N):
            consume(c.add("http://nederland.nl/%s" % i, "xml", "<xml>%s</xml>" % i))
            j = randint(0, i)
            data = c.getStream("http://nederland.nl/%s" % j, "xml").read()
            self.assertEquals("<xml>%s</xml>" % j, data)
            if i % 100 == 0:
                t1 = time()
                print i/(t1-t0)

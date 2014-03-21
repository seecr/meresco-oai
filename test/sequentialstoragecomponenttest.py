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
from meresco.oai import SequentialStorageComponent

from weightless.core import consume

class SequentialStorageComponentTest(SeecrTestCase):

    def testOne(self):
        c = SequentialStorageComponent(self.tempdir)
        self.assertEquals((False, False), c.isAvailable("an arbitratry identifier", partname="xml"))
        consume(c.add(identifier="an arbitratry identifier", partname="xml", data="<data/>"))
        self.assertEquals((True, True), c.isAvailable("an arbitratry identifier", partname="xml"))
        dataStream = c.getStream("an arbitratry identifier", "xml")
        self.assertEquals("<data/>", dataStream.read())

    def testTwo(self):
        c = SequentialStorageComponent(self.tempdir)
        consume(c.add("uri-1", "xml", "<xml1/>"))
        consume(c.add("uri-2", "xml", "<xml2/>"))
        self.assertEquals("<xml1/>", c.getStream("uri-1", "xml").read())
        self.assertEquals("<xml2/>", c.getStream("uri-2", "xml").read())

    def testDelete(self):
        c = SequentialStorageComponent(self.tempdir)
        consume(c.delete("uri-1"))
        consume(c.add("uri-1", "xml", "<xml1/>"))
        consume(c.add("uri-2", "xml", "<xml2/>"))
        self.assertEquals((True, True), c.isAvailable("uri-1", partname="xml"))
        self.assertEquals((True, True), c.isAvailable("uri-2", partname="xml"))
        consume(c.delete("uri-1"))
        self.assertEquals((False, False), c.isAvailable("uri-1", partname="xml"))
        self.assertEquals((True, True), c.isAvailable("uri-2", partname="xml"))

    def testDeleteFromExistingIndex(self):
        c = SequentialStorageComponent(self.tempdir)
        consume(c.add("uri-1", "xml", "<xml1/>"))
        c.handleShutdown()

        c = SequentialStorageComponent(self.tempdir)
        self.assertEquals((True, True), c.isAvailable("uri-1", partname="xml"))
        consume(c.delete("uri-1"))
        self.assertEquals((False, False), c.isAvailable("uri-1", partname="xml"))

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

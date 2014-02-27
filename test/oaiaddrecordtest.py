## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2010-2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012, 2014 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase, CallTrace
from meresco.oai import OaiAddRecord, OaiJazz, SequentialMultiStorage
from weightless.core import compose, consume, be
from os import makedirs

from StringIO import StringIO
from lxml.etree import parse
from meresco.core import Observable
from meresco.components import XmlPrintLxml

def parseLxml(aString):
    return parse(StringIO(aString)).getroot()

class OaiAddRecordTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.subject = OaiAddRecord()
        self.observer = CallTrace('observert')
        self.observer.getAllMetadataFormats = lambda: []
        self.subject.addObserver(self.observer)

    def testAdd(self):
        lxmlNode = parseLxml('<empty/>')
        list(compose(self.subject.add('id', 'partName', lxmlNode)))

        self.assertEquals(['addOaiRecord', 'add'], self.observer.calledMethodNames())
        self.assertEquals('id', self.observer.calledMethods[0].kwargs['identifier'])
        self.assertEquals([('partName', '', '')], self.observer.calledMethods[0].kwargs['metadataFormats'])
        self.assertEquals(set(), self.observer.calledMethods[0].kwargs['sets'])
        kwargs = self.observer.calledMethods[1].kwargs
        self.assertTrue(lxmlNode is kwargs.pop('lxmlNode'))
        self.assertEquals({'identifier':'id', 'partname':'partName'}, kwargs)

    def testAddSetInfo(self):
        header = parseLxml('<header xmlns="http://www.openarchives.org/OAI/2.0/"><setSpec>1</setSpec></header>')

        list(compose(self.subject.add('123', 'oai_dc', header)))

        self.assertEquals(['addOaiRecord', 'add'], self.observer.calledMethodNames())
        self.assertEquals('123', self.observer.calledMethods[0].kwargs['identifier'])
        self.assertEquals(set([('1','1')]), self.observer.calledMethods[0].kwargs['sets'])
        self.assertEquals([('oai_dc', '', "http://www.openarchives.org/OAI/2.0/")], self.observer.calledMethods[0].kwargs['metadataFormats'])

    def testAddSetInfoWithHeaderNotAsRootTag(self):
        xml = parseLxml('<someroot xmlns="root"><header xmlns="http://www.openarchives.org/OAI/2.0/"><setSpec>1</setSpec></header></someroot>')
        header = xml.xpath('/root:someroot/oai:header', namespaces = {
            'root':'root',
            'oai': 'http://www.openarchives.org/OAI/2.0/'})[0]

        list(compose(self.subject.add('123', 'oai_dc', header)))

        self.assertEquals(['addOaiRecord', 'add'], self.observer.calledMethodNames())
        self.assertEquals('123', self.observer.calledMethods[0].kwargs['identifier'])
        self.assertEquals(set([('1','1')]), self.observer.calledMethods[0].kwargs['sets'])
        self.assertEquals([('oai_dc', '', "http://www.openarchives.org/OAI/2.0/")], self.observer.calledMethods[0].kwargs['metadataFormats'])

    def testAddElementTree(self):
        header = parse(StringIO('<header xmlns="http://www.openarchives.org/OAI/2.0/"><setSpec>1</setSpec></header>'))

        list(compose(self.subject.add('123', 'oai_dc', header)))

        self.assertEquals(['addOaiRecord', 'add'], self.observer.calledMethodNames())
        self.assertEquals('123', self.observer.calledMethods[0].kwargs['identifier'])
        self.assertEquals(set([('1','1')]), self.observer.calledMethods[0].kwargs['sets'])
        self.assertEquals([('oai_dc', '', "http://www.openarchives.org/OAI/2.0/")], self.observer.calledMethods[0].kwargs['metadataFormats'])

    def testAddRecognizeNamespace(self):
        header = '<header xmlns="this.is.not.the.right.ns"><setSpec>%s</setSpec></header>'
        list(compose(self.subject.add('123', 'oai_dc', parseLxml(header % 1))))
        header = '<header xmlns="http://www.openarchives.org/OAI/2.0/"><setSpec>%s</setSpec></header>'
        list(compose(self.subject.add('124', 'oai_dc', parseLxml(header % 1))))
        self.assertEquals(['addOaiRecord', 'add', 'addOaiRecord', 'add'], self.observer.calledMethodNames())
        self.assertEquals([('oai_dc', '', "this.is.not.the.right.ns")], self.observer.calledMethods[0].kwargs['metadataFormats'])
        self.assertEquals([('oai_dc', '', "http://www.openarchives.org/OAI/2.0/")], self.observer.calledMethods[2].kwargs['metadataFormats'])

    def testMultipleHierarchicalSets(self):
        spec = "<setSpec>%s</setSpec>"
        header = '<header xmlns="http://www.openarchives.org/OAI/2.0/">%s</header>'
        list(compose(self.subject.add('124', 'oai_dc', parseLxml(header % (spec % '2:3' + spec % '3:4')))))
        self.assertEquals('124', self.observer.calledMethods[0].kwargs['identifier'])
        self.assertEquals([('oai_dc', '', "http://www.openarchives.org/OAI/2.0/")], self.observer.calledMethods[0].kwargs['metadataFormats'])
        self.assertEquals(set([('2:3', '2:3'), ('3:4', '3:4')]), self.observer.calledMethods[0].kwargs['sets'])

    def testMetadataPrefixes(self):
        list(compose(self.subject.add('456', 'oai_dc', parseLxml('<oai_dc:dc xmlns:oai_dc="http://oai_dc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
             xsi:schemaLocation="http://oai_dc http://oai_dc/dc.xsd"/>'))))
        self.assertEquals([('oai_dc', 'http://oai_dc/dc.xsd', 'http://oai_dc')], self.observer.calledMethods[0].kwargs['metadataFormats'])
        list(compose(self.subject.add('457', 'dc2', parseLxml('<oai_dc:dc xmlns:oai_dc="http://dc2"/>'))))
        self.assertEquals([('dc2', '', 'http://dc2')], self.observer.calledMethods[2].kwargs['metadataFormats'])

    def testMetadataPrefixesFromRootTag(self):
        list(compose(self.subject.add('456', 'oai_dc', parseLxml('''<oai_dc:dc
        xmlns:oai_dc="http://oai_dc"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://other
                            http://other.com/file.xsd
                            http://oai_dc
                            http://oai_dc/dc.xsd">
</oai_dc:dc>'''))))
        self.assertEquals([('oai_dc', 'http://oai_dc/dc.xsd', 'http://oai_dc')], self.observer.calledMethods[0].kwargs['metadataFormats'])

    def testIncompletePrefixInfo(self):
        list(compose(self.subject.add('457', 'dc2', parseLxml('<oai_dc/>'))))
        self.assertEquals([('dc2', '', '')], self.observer.calledMethods[0].kwargs['metadataFormats'])

    def testUseSequentialStorage(self):

        addrecord = OaiAddRecord(useSequentialStorage=True)
        jazz =  OaiJazz(self.tempdir)
        makedirs(self.tempdir + '/1')
        storage = SequentialMultiStorage(self.tempdir + '/1')
        observable = be((Observable(),
                (addrecord,
                    (jazz,),
                    (XmlPrintLxml(fromKwarg='lxmlNode', toKwarg='data', pretty_print=False),
                        (storage,)
                    )
                )
            ))

        t0 = jazz._newStamp()
        consume(observable.all.add("id0", "oai_dc", parseLxml("<xml/>")))
        consume(observable.all.add("id0", "other", parseLxml("<json/>")))
        t1 = jazz._newStamp()
        t, data = storage.iterData("oai_dc", 0, None).next()
        self.assertTrue(t0 < int(t) < t1, t)
        self.assertEquals("<xml/>", data)
        t, data = storage.iterData("other", 0, None).next()
        self.assertTrue(t0 < int(t) < t1, t)
        self.assertEquals("<json/>", data)



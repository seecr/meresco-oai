## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010-2011 Seek You Too (CQ2) http://www.cq2.nl
#    Copyright (C) 2010-2011 Stichting Kennisnet http://www.kennisnet.nl
#
#    This file is part of Meresco Oai.
#
#    Meresco Oai is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    Meresco Oai is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Meresco Oai; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from cq2utils import CQ2TestCase, CallTrace
from oaitestcase import assertValidOai

from meresco.oai import OaiPmh, OaiJazz
from meresco.core import Observable, be
from meresco.components.http.utils import CRLF
from meresco.components import StorageComponent
from os.path import join
from urllib import urlencode
from lxml.etree import parse, tostring
from StringIO import StringIO
from weightless.core import compose

BATCHSIZE = 10
class OaiPmhJazzTest(CQ2TestCase):
    def setUp(self):
        CQ2TestCase.setUp(self)
        jazz = OaiJazz(join(self.tempdir, 'jazz'))
        storage = StorageComponent(join(self.tempdir, 'storage'))
        self.root = be((Observable(),
            (OaiPmh(repositoryName='Repository', adminEmail='admin@cq2.nl', batchSize=BATCHSIZE),
                (jazz,),
                (storage,)
            )
        ))
        for i in xrange(20):
            recordId = 'record:id:%02d' % i
            metadataFormats = [('oai_dc', 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd', 'http://www.openarchives.org/OAI/2.0/oai_dc/')]
            storage.add(identifier=recordId, partname='oai_dc', data='<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:identifier>%s</dc:identifier></oai_dc:dc>' % recordId)
            if i >= 10:
                metadataFormats.append(('prefix2', 'http://example.org/prefix2/?format=xsd&prefix=2','http://example.org/prefix2/'))
                storage.add(identifier=recordId, partname='prefix2', data='<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:subject>%s</dc:subject></oai_dc:dc>' % recordId)
            sets = []
            if i >= 5:
                sets.append(('setSpec%s' % ((i//5)*5), 'setName'))
            if 5 <= i < 10:
                sets.append(('hierarchical:set', 'hierarchical set'))
            if 10 <= i < 15:
                sets.append(('hierarchical', 'hierarchical toplevel only'))
            jazz.addOaiRecord(recordId, sets=sets, metadataFormats=metadataFormats)
            if i % 5 == 0:
                jazz.delete(recordId)

    def _request(self, from_=None, **arguments):
        if from_:
            arguments['from'] = from_
        header, body = ''.join(compose(self.root.all.handleRequest(
                RequestURI='http://example.org/oai?' + urlencode(arguments, doseq=True),
                Headers={},
                Client=('127.0.0.1', 1324),
                Method="GET",
                port=9000,
                arguments=arguments,
                path='/oai',
            ))).split(CRLF * 2)
        parsedBody = parse(StringIO(body))
        assertValidOai(parsedBody)
        return header, parsedBody

    def testBugListRecordsReturnsDoubleValueOnNoRecordsMatch(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['oai_dc'], from_=['9999-01-01'])
        self.assertEquals(['noRecordsMatch'], xpath(body, '/oai:OAI-PMH/oai:error/@code'), tostring(body, pretty_print=True))

    def testListRecords(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['prefix2'])
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEquals(10, len(records))
        self.assertEquals(['record:id:11'], xpath(records[1], 'oai:header/oai:identifier/text()'))
        self.assertEquals(['record:id:11'], xpath(records[1], 'oai:metadata/oai_dc:dc/dc:subject/text()'), tostring(records[1]))
        self.assertEquals(['hierarchical', 'setSpec10'], sorted(xpath(records[1], 'oai:header/oai:setSpec/text()')))
        deletedRecords = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record[oai:header/@status="deleted"]')
        self.assertEquals(2, len(deletedRecords))
        self.assertEquals([0,0], [len(xpath(r, 'oai:metadata')) for r in deletedRecords])
        self.assertEquals(['hierarchical', 'setSpec10'], sorted(xpath(deletedRecords[0], 'oai:header/oai:setSpec/text()')))

    def testListRecordsWithResumptionToken(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['oai_dc'])
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEquals(10, len(records))
        resumptionToken = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')[0]
        header, body = self._request(verb=['ListRecords'], resumptionToken=[resumptionToken])
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEquals(10, len(records))
        self.assertEquals(0, len(xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')))

    def testGetRecordNotAvailable(self):
        header, body = self._request(verb=['GetRecord'], metadataPrefix=['oai_dc'], identifier=['doesNotExist'])

        error = xpath(body, '/oai:OAI-PMH/oai:error')[0]
        self.assertEquals('idDoesNotExist', error.attrib['code'])
        self.assertEquals('The value of the identifier argument is unknown or illegal in this repository.', error.text)

    def testGetRecord(self):
        header, body = self._request(verb=['GetRecord'], metadataPrefix=['oai_dc'], identifier=['record:id:11'])

        self.assertEquals(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        records = xpath(body, '/oai:OAI-PMH/oai:GetRecord/oai:record')
        self.assertEquals(1, len(records))
        self.assertEquals(['record:id:11'], xpath(records[0], 'oai:header/oai:identifier/text()'))
        self.assertEquals(['record:id:11'], xpath(records[0], 'oai:metadata/oai_dc:dc/dc:identifier/text()'), tostring(records[0]))
        self.assertEquals(['hierarchical', 'setSpec10'], sorted(xpath(records[0], 'oai:header/oai:setSpec/text()')))

    def testGetRecordDeleted(self):
        header, body = self._request(verb=['GetRecord'], metadataPrefix=['oai_dc'], identifier=['record:id:10'])

        self.assertEquals(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        records = xpath(body, '/oai:OAI-PMH/oai:GetRecord/oai:record')
        self.assertEquals(1, len(records))
        self.assertEquals(['record:id:10'], xpath(records[0], 'oai:header/oai:identifier/text()'))
        self.assertEquals(0, len(xpath(records[0], 'oai:metadata')))
        self.assertEquals(['hierarchical', 'setSpec10'], sorted(xpath(records[0], 'oai:header/oai:setSpec/text()')))

    def testListAllMetadataFormats(self):
        header, body = self._request(verb=['ListMetadataFormats'])

        self.assertEquals(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        formats = xpath(body, '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat')
        self.assertEquals(2, len(formats), tostring(body, pretty_print=True))
        self.assertEquals(['oai_dc', 'prefix2'], [xpath(f, 'oai:metadataPrefix/text()')[0] for f in formats])
        self.assertEquals(['http://www.openarchives.org/OAI/2.0/oai_dc.xsd', 'http://example.org/prefix2/?format=xsd&prefix=2'], [xpath(f, 'oai:schema/text()')[0] for f in formats])
        self.assertEquals(['http://www.openarchives.org/OAI/2.0/oai_dc/', 'http://example.org/prefix2/'], [xpath(f, 'oai:metadataNamespace/text()')[0] for f in formats])

    def testListMetadataFormatsForIdentifier(self):
        header, body = self._request(verb=['ListMetadataFormats'], identifier=['record:id:01'])

        self.assertEquals(0, len(xpath(body, '/oai:OAI-PMH/oai:error')), tostring(body, pretty_print=True))
        formats = xpath(body, '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat')
        self.assertEquals(1, len(formats), tostring(body, pretty_print=True))
        self.assertEquals(['oai_dc'], xpath(formats[0], 'oai:metadataPrefix/text()'))

    def testListMetadataFormatsForWrongIdentifier(self):
        header, body = self._request(verb=['ListMetadataFormats'], identifier=['does:not:exist'])

        self.assertEquals(['idDoesNotExist'], xpath(body, '/oai:OAI-PMH/oai:error/@code'), tostring(body, pretty_print=True))

    def testListAllSets(self):
        header, body = self._request(verb=['ListSets'])

        self.assertEquals(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        sets = xpath(body, '/oai:OAI-PMH/oai:ListSets/oai:set/oai:setSpec/text()')
        self.assertEquals(set(['setSpec5', 'setSpec10', 'setSpec15', 'hierarchical', 'hierarchical:set']), set(sets), tostring(body, pretty_print=True))

    def testListMetadataFormatsForWrongIdentifier(self):
        self.root = be((Observable(),
            (OaiPmh(repositoryName='Repository', adminEmail='admin@cq2.nl', batchSize=BATCHSIZE),
                (OaiJazz(join(self.tempdir, 'empty'),),)
            )
        ))

        header, body = self._request(verb=['ListSets'])

        self.assertEquals(['noSetHierarchy'], xpath(body, '/oai:OAI-PMH/oai:error/@code'), tostring(body, pretty_print=True))

def xpath(node, path):
    return node.xpath(path, namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/',
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
        'dc': 'http://purl.org/dc/elements/1.1/',
        })


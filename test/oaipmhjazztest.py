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
            metadataFormats = [ ('oai_dc', 'dc-schema', 'dc-namespace')]
            storage.add(identifier=recordId, partname='oai_dc', data='<data xmlns="my:data">%s</data>' % recordId)
            if i >= 10:
                metadataFormats.append(('prefix2', 'schema2', 'namespace2'))
                storage.add(identifier=recordId, partname='prefix2', data='<p2:data xmlns:p2="my:prefix2">%s</p2:data>' % recordId)
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
        head, body = ''.join(compose(self.root.all.handleRequest(
                RequestURI='http://example.org/oai?' + urlencode(arguments, doseq=True),
                Headers={},
                Client=('127.0.0.1', 1324),
                Method="GET",
                port=9000,
                arguments=arguments,
                path='/oai',
            ))).split(CRLF * 2)
        return head, parse(StringIO(body))

    def testBugListRecordsReturnsDoubleValueOnNoRecordsMatch(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['oai_dc'], from_=['9999-01-01'])
        self.assertEquals(['noRecordsMatch'], xpath(body, '/oai:OAI-PMH/oai:error/@code'), tostring(body, pretty_print=True))

    def testListRecords(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['prefix2'])
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEquals(10, len(records))
        self.assertEquals(['record:id:11'], xpath(records[1], 'oai:header/oai:identifier/text()'))
        self.assertEquals(['record:id:11'], xpath(records[1], 'oai:metadata/p2:data/text()'))
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



def xpath(node, path):
    return node.xpath(path, namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/',
        'p2': 'my:prefix2',
        'data': 'my:data',})

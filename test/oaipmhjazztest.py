## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
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
from meresco.components.http.webrequestserver import WebRequestServer
from meresco.components.http.utils import CRLF
from urllib import urlencode
from lxml.etree import parse
from StringIO import StringIO

class OaiPmhJazzTest(CQ2TestCase):
    def setUp(self):
        super(OaiPmhJazzTest, self).setUp()
        self.jazz = OaiJazz(self.tempdir)
        self.root = be((Observable(),
            (WebRequestServer(),
                (OaiPmh(repositoryName='Repository', adminEmail='admin@cq2.nl'),
                    (self.jazz,)
                )
            )
        ))
        for i in xrange(20):
            recordId = 'record:id:%02d' % i
            metadataFormats = [ ('oai_dc', 'dc-schema', 'dc-namespace')]
            if i >= 10:
                metadataFormats.append(('prefix2', 'schema2', 'namespace2'))
            sets = []
            if i >= 5:
                sets.append(('setSpec%s' % ((i//5)*5), 'setName'))
            if 5 <= i < 10:
                sets.append(('hierarchical:set', 'hierarchical set'))
            if 10 <= i < 15:
                sets.append(('hierarchical', 'hierarchical toplevel only'))
            self.jazz.addOaiRecord(recordId, sets=sets, metadataFormats=metadataFormats)

    def handleRequest(self, **kwargs):
        return self.root.all.handleRequest(
                RequestURI='http://example.org/oai?' + urlencode(kwargs),
                Headers={},
                Client=('127.0.0.1', 1324)
               )

    def testBugListRecordsReturnsDoubleValueOnNoRecordsMatch(self):
        head, body = ''.join(self.handleRequest(verb='ListRecords', metadataPrefix='oai_dc', **{'from':'9999-01-01'})).split(CRLF * 2)
        self.assertTrue(parse(StringIO(body)))


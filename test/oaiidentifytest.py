## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
#    Copyright (C) 2010 Maastricht University Library
#        http://www.maastrichtuniversity.nl/web/Library/home.htm
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

from meresco.core import Observable
from weightless import compose
from cq2utils import CallTrace

from meresco.oai.oaiidentify import OaiIdentify
from meresco.components.http.utils import CRLF
from oaitestcase import OaiTestCase
from StringIO import StringIO
from lxml.etree import parse

class OaiIdentifyTest(OaiTestCase):
    def getSubject(self):
        return OaiIdentify()

    def testExtraDescription(self):
        observer = CallTrace('observer')
        observer.methods['description'] = lambda: (f for f in ['<description>','data','</description>'])
        self.subject.addObserver(observer)

        header, body = ''.join(compose(self.subject.identify(arguments={'verb':['Identify']}, **self.httpkwargs))).split(CRLF*2)
        result = parse(StringIO(body))

        descriptions = result.xpath('/oai:OAI-PMH/oai:Identify/oai:description', namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})
        self.assertEquals(2, len(descriptions))
        self.assertEquals('data', descriptions[1].text)
        self.assertEquals(['description'], [m.name for m in observer.calledMethods])

        


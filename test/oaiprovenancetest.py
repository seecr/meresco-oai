## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2012, 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

from seecr.test import CallTrace, SeecrTestCase
from StringIO import StringIO

from weightless.core import compose
from meresco.core import Observable
from meresco.oai4.oaiprovenance import OaiProvenance


class OaiProvenanceTest(SeecrTestCase):
    def testCacheStorageResults(self):
        observable = Observable()
        provenance = OaiProvenance(
            nsMap = {},
            baseURL = ('meta', 'meta/repository/baseurl/text()'),
            harvestDate = ('meta', 'meta/repository/harvestDate/text()'),
            metadataNamespace = ('meta', 'meta/repository/metadataNamespace/text()'),
            identifier = ('header','header/identifier/text()'),
            datestamp = ('header', 'header/datestamp/text()')
        )
        observable.addObserver(provenance)
        storage = MockStorage()
        observer = storage
        provenance.addObserver(observer)

        self.assertEquals(0, storage.timesCalled)
        result = ''.join(list(compose(observable.any.provenance("recordId"))))
        self.assertEquals(2, storage.timesCalled)

    def testProvenance(self):
        observable = Observable()
        provenance = OaiProvenance(
            nsMap = {'oai_dc': "http://www.openarchives.org/OAI/2.0/"},
            baseURL = ('meta', '/meta/repository/baseurl/text()'),
            harvestDate = ('meta', '/meta/repository/harvestDate/text()'),
            metadataNamespace = ('meta', '/meta/repository/metadataNamespace/text()'),
            identifier = ('header','/oai_dc:header/oai_dc:identifier/text()'),
            datestamp = ('header', '/oai_dc:header/oai_dc:datestamp/text()')
        )
        observable.addObserver(provenance)
        observer = MockStorage()
        provenance.addObserver(observer)

        result = ''.join(list(compose(observable.any.provenance("recordId"))))
        self.assertEqualsWS(result, """<provenance xmlns="http://www.openarchives.org/OAI/2.0/provenance"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/provenance
                      http://www.openarchives.org/OAI/2.0/provenance.xsd">

<originDescription harvestDate="HARVESTDATE" altered="true">
  <baseURL>BASEURL</baseURL>
  <identifier>IDENTIFIER</identifier>
  <datestamp>DATESTAMP</datestamp>
  <metadataNamespace>METADATANAMESPACE</metadataNamespace>
</originDescription>
</provenance>""")

    def testNoOutputIfValueMissing(self):
        observable = Observable()
        provenance = OaiProvenance(
            nsMap = {},
            baseURL = ('meta', 'meta/repository/baseurl'),
            harvestDate = ('meta', 'meta/does/not/exist'),
            metadataNamespace = ('meta', 'meta/repository/metadataNamespace'),
            identifier = ('header','header/identifier'),
            datestamp = ('header', 'header/datestamp')
        )
        observable.addObserver(provenance)
        observer = MockStorage()
        provenance.addObserver(observer)

        result = ''.join(list(compose(observable.any.provenance("recordId"))))
        self.assertEquals('', result)

class MockStorage(object):
    def __init__(self):
        self.timesCalled = 0

    def getStream(self, ident, partname):
        self.timesCalled += 1
        if partname == 'meta':
            return StringIO("<meta><repository><metadataNamespace>METADATANAMESPACE</metadataNamespace><baseurl>BASEURL</baseurl><harvestDate>HARVESTDATE</harvestDate></repository></meta>")
        elif partname == 'header':
            return StringIO("""<header xmlns="http://www.openarchives.org/OAI/2.0/">
    <identifier>IDENTIFIER</identifier>
    <datestamp>DATESTAMP</datestamp>

  </header>""")


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
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012, 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from lxml.etree import parse
from StringIO import StringIO

from seecr.test import SeecrTestCase, CallTrace

from meresco.oai4.oairecord import OaiRecord

from weightless.core import compose

class OaiRecordTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.oaiRecord = OaiRecord()
        self.observer = CallTrace('Observer')
        self.oaiRecord.addObserver(self.observer)
        self.observer.returnValues['isDeleted'] = False
        self.observer.returnValues['getDatestamp'] = '2011-03-25T10:45:00Z'
        self.observer.returnValues['getSets'] = ['set0', 'set1']
        self.observer.returnValues['yieldRecord'] = (f for f in ['<da','ta/>'])
        self.observer.returnValues['provenance'] = (f for f in [])
        
    def testRecord(self):
        result = ''.join(compose(self.oaiRecord.oaiRecord(recordId='id', metadataPrefix='oai_dc')))

        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
<metadata>
    <data/>
</metadata>
</record>""", result)
        self.assertEquals(["isDeleted('id')", "getDatestamp('id')", "getSets('id')", "yieldRecord('id', 'oai_dc')", "provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testRecordIsDeleted(self):
        self.observer.returnValues['isDeleted'] = True
        result = ''.join(compose(self.oaiRecord.oaiRecord(recordId='id', metadataPrefix='oai_dc')))

        self.assertEqualsWS("""<record>
<header status="deleted">
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
</record>""", result)
        self.assertEquals(["isDeleted('id')", "getDatestamp('id')", "getSets('id')"], [str(m) for m in self.observer.calledMethods])

    def testRecordsWithoutSets(self):
        self.observer.returnValues['getSets'] = (f for f in [])
        result = ''.join(compose(self.oaiRecord.oaiRecord(recordId='id', metadataPrefix='oai_dc')))

        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
</header>
<metadata>
    <data/>
</metadata>
</record>""", result)
        self.assertEquals(["isDeleted('id')", "getDatestamp('id')", "getSets('id')", "yieldRecord('id', 'oai_dc')", "provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testRecordWithProvenance(self):
        self.observer.returnValues['provenance'] = (f for f in ['PROV','ENANCE'])
        result = ''.join(compose(self.oaiRecord.oaiRecord(recordId='id', metadataPrefix='oai_dc')))

        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
<metadata>
    <data/>
</metadata>
<about>PROVENANCE</about>
</record>""", result)
        self.assertEquals(["isDeleted('id')", "getDatestamp('id')", "getSets('id')", "yieldRecord('id', 'oai_dc')", "provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testDeletedRecordWithProvenance(self):
        self.observer.returnValues['isDeleted'] = True
        self.observer.returnValues['provenance'] = (f for f in ['PROV','ENANCE'])
        result = ''.join(compose(self.oaiRecord.oaiRecord(recordId='id&0', metadataPrefix='oai_dc')))

        self.assertEqualsWS("""<record>
<header status="deleted">
    <identifier>id&amp;0</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
</record>""", result)
        self.assertEquals(["isDeleted('id&0')", "getDatestamp('id&0')", "getSets('id&0')"], [str(m) for m in self.observer.calledMethods])


    def testRecordForListIdentifiers(self):
        result = ''.join(compose(self.oaiRecord.oaiRecordHeader(recordId='id', metadataPrefix='oai_dc')))

        self.assertEqualsWS("""<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>""", result)
        self.assertEquals(["isDeleted('id')", "getDatestamp('id')", "getSets('id')"], [str(m) for m in self.observer.calledMethods])




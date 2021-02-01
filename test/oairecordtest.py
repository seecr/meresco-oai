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
# Copyright (C) 2011, 2018, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2012-2014, 2016-2018, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 SURFmarket https://surf.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from weightless.core import compose, DeclineMessage

from seecr.test import SeecrTestCase, CallTrace

from meresco.oai.oairecord import OaiRecord
from meresco.oai.oairepository import OaiRepository

from mockoaijazz import MockRecord


class OaiRecordTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.setUpOaiRecord()

    def setUpOaiRecord(self, **kwargs):
        self.oaiRecord = OaiRecord(**kwargs)
        self.observer = CallTrace('Observer')
        self.oaiRecord.addObserver(self.observer)
        self.observer.returnValues['provenance'] = (f for f in [])
        self.observer.returnValues['getData'] = '<data/>'

    def testRecord(self):
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id'), metadataPrefix='oai_dc', fetchedRecords=None)))
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
        self.assertEqual(["getData(identifier='id', name='oai_dc')", "provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testRecordWithRetrieveData(self):
        def getData(*_, **__):
            raise DeclineMessage()
        def retrieveData(*_, **__):
            return '<retrieved/>'
            yield
        self.observer.methods['retrieveData'] = retrieveData
        self.observer.methods['getData'] = getData
        self.observer.returnValues.pop('getData')
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id'), metadataPrefix='oai_dc', fetchedRecords=None)))
        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
<metadata>
    <retrieved/>
</metadata>
</record>""", result)
        self.assertEqual([
            "getData(identifier='id', name='oai_dc')",
            "retrieveData(identifier='id', name='oai_dc')",
            "provenance('id')"
        ], [str(m) for m in self.observer.calledMethods])

    def testRecordWithFetchedRecords(self):
        record = MockRecord('id')
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=record, metadataPrefix='oai_dc', fetchedRecords={record.identifier: "<the>data</the>", 'abc': '<some>other data</some>'})))
        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
<metadata>
    <the>data</the>
</metadata>
</record>""", result)
        self.assertEqual(["provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testPreciseDatestamp(self):
        self.setUpOaiRecord(preciseDatestamp=True)
        record = MockRecord('id')
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=record, metadataPrefix='oai_dc', fetchedRecords={record.identifier: "<the>data</the>", 'abc': '<some>other data</some>'})))
        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00.123Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
<metadata>
    <the>data</the>
</metadata>
</record>""", result)

    def testRecordIsDeleted(self):
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id', deleted=True), metadataPrefix='oai_dc')))
        self.assertEqualsWS("""<record>
<header status="deleted">
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
</record>""", result)
        self.assertEqual([], [str(m) for m in self.observer.calledMethods])

    def testRecordWithDeleteInSetsSupport(self):
        self.setUpOaiRecord(deleteInSets=True)
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id', sets={'set0', 'set1'}, deletedSets={'set1'}), metadataPrefix='oai_dc')))
        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec status="deleted">set1</setSpec>
</header>
<metadata><data/></metadata>
</record>""", result)


    def testRecordsWithoutSets(self):
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id', sets=[]), metadataPrefix='oai_dc')))
        self.assertEqualsWS("""<record>
<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
</header>
<metadata>
    <data/>
</metadata>
</record>""", result)
        self.assertEqual(["getData(identifier='id', name='oai_dc')", "provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testRecordWithProvenance(self):
        self.observer.returnValues['provenance'] = (f for f in ['PROV','ENANCE'])
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id'), metadataPrefix='oai_dc')))
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
        self.assertEqual(["getData(identifier='id', name='oai_dc')", "provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testDeletedRecordWithProvenance(self):
        self.observer.returnValues['provenance'] = (f for f in ['PROV','ENANCE'])
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id&0', deleted=True), metadataPrefix='oai_dc')))
        self.assertEqualsWS("""<record>
<header status="deleted">
    <identifier>id&amp;0</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
</record>""", result)
        self.assertEqual([], [str(m) for m in self.observer.calledMethods])

    def testRecordForListIdentifiers(self):
        result = ''.join(compose(self.oaiRecord.oaiRecordHeader(record=MockRecord('id'), metadataPrefix='oai_dc', kwarg0="ignored")))
        self.assertEqualsWS("""<header>
    <identifier>id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>""", result)
        self.assertEqual([], [str(m) for m in self.observer.calledMethods])

    def testRecordWithRepositoryIdentifier(self):
        self.setUpOaiRecord(repository=OaiRepository(identifier='example.org'))
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=MockRecord('id'), metadataPrefix='oai_dc', fetchedRecords=None)))
        self.assertEqualsWS("""<record>
<header>
    <identifier>oai:example.org:id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
<metadata>
    <data/>
</metadata>
</record>""", result)
        self.assertEqual(["getData(identifier='id', name='oai_dc')", "provenance('id')"], [str(m) for m in self.observer.calledMethods])

    def testRecordWithFetchedRecordsWithRepositoryIdentifier(self):
        self.setUpOaiRecord(repository=OaiRepository(identifier='example.org'))
        record = MockRecord('id')
        result = ''.join(compose(self.oaiRecord.oaiRecord(record=record, metadataPrefix='oai_dc', fetchedRecords={record.identifier: "<the>data</the>", 'abc': '<some>other data</some>'})))
        self.assertEqualsWS("""<record>
<header>
    <identifier>oai:example.org:id</identifier>
    <datestamp>2011-03-25T10:45:00Z</datestamp>
    <setSpec>set0</setSpec>
    <setSpec>set1</setSpec>
</header>
<metadata>
    <the>data</the>
</metadata>
</record>""", result)
        self.assertEqual(["provenance('id')"], [str(m) for m in self.observer.calledMethods])

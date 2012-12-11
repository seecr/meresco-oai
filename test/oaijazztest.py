# -*- coding: utf-8 -*-
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
# Copyright (C) 2011-2012 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from seecr.test.io import stderr_replaced

from os import listdir, remove
from os.path import isfile, join
from time import time, strptime, sleep
from calendar import timegm

from meresco.oai import OaiJazz, OaiAddRecord, stamp2zulutime
from meresco.oai.oaijazz import _flattenSetHierarchy, RecordId, SETSPEC_SEPARATOR
from meresco.oai.oailist import OaiList
from StringIO import StringIO
from lxml.etree import parse
from meresco.core import Observable, Transparent
from weightless.core import be, compose
from weightless.io import Suspend

parseLxml = lambda s: parse(StringIO(s)).getroot()


class OaiJazzTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.jazz = OaiJazz(self.tempdir)
        self._originalNewStamp = self.jazz._newStamp
        self.stampNumber = self.orginalStampNumber = int((timegm((2008, 07, 06, 05, 04, 03, 0, 0, 1))+.123456)*1000000)
        def stamp():
            result = self.stampNumber
            self.stampNumber += 1
            return result
        self.jazz._newStamp = stamp
        self.oaiAddRecord = OaiAddRecord()
        self.oaiAddRecord.addObserver(self.jazz)

    def testObservableName(self):
        jazz = OaiJazz(self.tempdir, name='someName')
        observable = Observable()
        observable.addObserver(jazz)

        self.assertEquals('someName', jazz.observable_name())

        result = observable.call['someName'].getNrOfRecords()
        self.assertEquals(0, result)

    def testOriginalStamp(self):
        jazz = OaiJazz(self.tempdir)
        stamp0 = jazz._newStamp()
        sleep(0.0001)
        stamp1 = jazz._newStamp()
        self.assertTrue(stamp0 < stamp1, "Expected %s < %s" % (stamp0, stamp1))

    def testResultsStored(self):
        self.jazz.addOaiRecord(identifier='oai://1234?34', sets=[], metadataFormats=[('prefix', 'schema', 'namespace')])
        myJazz = OaiJazz(self.tempdir)
        recordIds = myJazz.oaiSelect(prefix='prefix')
        self.assertEquals('oai://1234?34', recordIds.next())

    def testAddOaiRecordEmptyIdentifier(self):
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord("", metadataFormats=[('prefix', 'schema', 'namespace')]))
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord(None, metadataFormats=[('prefix', 'schema', 'namespace')]))

    def xtestPerformanceTestje(self):
        t0 = time()
        lastTime = t0
        for i in xrange(1, 10**4 + 1):
            self.jazz.addOaiRecord('id%s' % i, sets=[('setSpec%s' % ((i / 100)*100), 'setName')], metadataFormats=[('prefix','schema', 'namespace')])
            if i%1000 == 0 and i > 0:
                tmp = time()
                print '%7d' % i, '%.4f' % (tmp - lastTime), '%.6f' % ((tmp - t0)/float(i))
                lastTime = tmp
        t1 = time()
        ids = self.jazz.oaiSelect(sets=['setSpec9500'],prefix='prefix')
        firstId = ids.next()
        allids = [firstId]
        t2 = time()
        allids.extend(list(ids))
        self.assertEquals(100, len(allids))
        t3 = time()
        for identifier in allids:
            list(self.jazz.getSets(identifier))
        t4 = time()
        jazz = OaiJazz(self.tempdir)
        t5 = time()
        print t1 - t0, t2 - t1, t3 -t2, t3 -t1, t4 - t3, t5 - t4
        # a set of 10 million records costs 3.9 seconds (Without any efficiency things applied
        # it costs 0.3 seconds with 1 million records
        # retimed it at 2009-01-13:
        #  1 * 10**6 oaiSelect took 3.7 seconds
        #  1 * 10**7 oaiSelect took 37.3 seconds
        # New optimization with And, Or Iterator
        #  1 * 10**6 oaiSelect took 0.363089084625
        #  1 * 10**7 oaiSelect took 0.347623825073
        # New implementation with LuceneDict and SortedFileList with delete support
        #  insert of 10*4 took 153 secs
        #  oaiSelect took 0.1285
        # 2009-11-18: new implementation of lookup of sets for an identifier (getSets) using
        # berkeleydict.
        # previous getSets(id) for 100 identifiers took 1.10 secs
        # after getSets(id) for 100 identifiers took 0.004 secs
        # penalty on insertion of 10.000 records previous 22 secs, after 27 secs
        # Same test but with 100.000 records (ulimit must be increased)
        # 285.413653135 0.240143060684 0.0137410163879 0.253884077072 0.00416588783264 0.167983055115
        # 237.773926973 0.240620851517 0.0134921073914 0.254112958908 14.3589520454 0.160785913467

    def testGetDatestamp(self):
        self.jazz.addOaiRecord('123', metadataFormats=[('oai_dc', 'schema', 'namespace')])
        self.assertEquals('2008-07-06T05:04:03Z', self.jazz.getDatestamp('123'))

    def testGetPreciseDatestamp(self):
        jazz = OaiJazz(self.tempdir, preciseDatestamp=True)
        jazz._newStamp = self.jazz._newStamp
        jazz.addOaiRecord('123', metadataFormats=[('oai_dc', 'schema', 'namespace')])
        self.assertEquals('2008-07-06T05:04:03.123456Z', jazz.getDatestamp('123'))

    def testDeleteNonExistingRecords(self):
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        list(compose(self.jazz.delete('notExisting')))
        jazz2 = OaiJazz(self.tempdir)
        self.assertEquals(None, jazz2.getUnique('notExisting'))

    def testDeleteEmptyIdentifier(self):
        self.assertRaises(ValueError, lambda: list(compose(self.jazz.delete(""))))
        self.assertRaises(ValueError, lambda: list(compose(self.jazz.delete(None))))

    def testMarkDeleteOfNonExistingRecordInGivenPrefixes(self):
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        jazz = OaiJazz(self.tempdir, alwaysDeleteInPrefixes=["aprefix"])
        list(compose(jazz.delete('notExisting')))
        self.assertEquals(['notExisting'], list(jazz.oaiSelect(prefix='aprefix')))
        self.assertEquals(['existing'], list(jazz.oaiSelect(prefix='prefix')))
        list(compose(jazz.delete('existing')))
        self.assertEquals(['notExisting', 'existing'], list(jazz.oaiSelect(prefix='aprefix')))

    def testDoNotPerformSuperfluousDeletes(self):
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz._stamp2identifier = CallTrace('mockdict', returnValues={'getKeysFor': None, '__delitem__':None})
        list(compose(self.jazz.delete('notExisting')))
        self.assertFalse("__delitem__" in str(self.jazz._stamp2identifier.calledMethods))

    def testPurgeRecord(self):
        self.jazz = OaiJazz(self.tempdir, persistentDelete=False)
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        stampId = self.jazz.getUnique('existing')
        self.jazz.purge('existing')
        jazz2 = OaiJazz(self.tempdir)
        self.assertEquals(None, jazz2.getUnique('existing'))
        self.assertTrue(stampId not in jazz2._tombStones)
        for prefix, stampIds in jazz2._prefixes.items():
            self.assertTrue(stampId not in stampIds)
        for set, stampIds in jazz2._sets.items():
            self.assertTrue(stampId not in stampIds)
        self.assertTrue('existing' not in jazz2._identifier2setSpecs)

    def testPurgeNotAllowedIfDeletesArePersistent(self):
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        try:
            self.jazz.purge('existing')
            self.fail("Should fail on purging because deletes are persistent")
        except KeyError, e:
            self.assertEquals("'Purging of records is not allowed with persistent deletes.'", str(e))

    # What happens if you do addOaiRecord('id1', prefix='aap') and afterwards
    #   addOaiRecord('id1', prefix='noot')
    # According to the specification:
    # Deleted status is a property of individual records. Like a normal record, a deleted record is identified by a unique identifier, a metadataPrefix and a datestamp. Other records, with different metadataPrefix but the same unique identifier, may remain available for the item.

    def testDeleteIsPersistent(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        list(compose(self.jazz.delete('42')))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='oai_dc')))
        jazz2 = OaiJazz(self.tempdir)
        self.assertTrue(jazz2.isDeleted('42'))
        self.assertEquals(['42'], list(jazz2.oaiSelect(prefix='oai_dc')))

    def testAddOaiRecordPersistent(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('prefix','schema', 'namespace')], sets=[('setSpec', 'setName')])
        jazz2 = OaiJazz(self.tempdir)
        self.assertEquals(['42'], list(jazz2.oaiSelect(prefix='prefix', sets=['setSpec'])))

    def testUnicodeIdentifier(self):
        self.jazz.addOaiRecord(u'ë', metadataFormats=[('prefix','schema', 'namespace')], sets=[('setSpec', 'setName')])
        jazz2 = OaiJazz(self.tempdir)
        self.assertEquals(['ë'], list(jazz2.oaiSelect(prefix='prefix', sets=['setSpec'])))
        self.assertFalse(jazz2.isDeleted(u'ë'))
        list(compose(jazz2.delete(u'ë')))
        self.assertTrue(jazz2.isDeleted('ë'))
        self.assertTrue(jazz2.isDeleted(u'ë'))
        self.assertNotEquals(None, jazz2.getDatestamp(u'ë'))
        self.assertNotEquals(None, jazz2.getUnique(u'ë'))
        self.assertEquals(['setSpec'], jazz2.getSets(u'ë'))
        self.assertEquals(['prefix'], list(jazz2.getPrefixes(u'ë')))

        jazz3 = OaiJazz(self.tempdir, persistentDelete=False)
        jazz3.purge(u'ë')
        self.assertEquals([], list(jazz3.oaiSelect(prefix='prefix', sets=['setSpec'])))

    def testWeirdSetOrPrefixNamesDoNotMatter(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('/%^!@#$   \n\t','schema', 'namespace')], sets=[('set%2Spec\n\n', 'setName')])
        jazz2 = OaiJazz(self.tempdir)
        self.assertEquals(['42'], list(jazz2.oaiSelect(prefix='/%^!@#$   \n\t', sets=['set%2Spec\n\n'])))


    def testOaiSelectWithFromAfterEndOfTime(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        result = self.jazz.oaiSelect(prefix='oai_dc', oaiFrom='9999-01-01T00:00:00Z')
        self.assertEquals(0,len(list(result)))

    def testDeleteIncrementsDatestampAndUnique(self):
        self.jazz.addOaiRecord('23', metadataFormats=[('oai_dc','schema', 'namespace')])
        stamp = self.jazz.getDatestamp('23')
        unique = self.jazz.getUnique('23')
        self.stampNumber += 1234567890 # increaseTime
        list(compose(self.jazz.delete('23')))
        self.assertNotEqual(stamp, self.jazz.getDatestamp('23'))
        self.assertNotEquals(unique, int(self.jazz.getUnique('23')))

    def testTimeUpdateRaisesErrorButLeavesIndexCorrect(self):
        self.jazz._newStamp = self._originalNewStamp
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        self.jazz._newestStamp += 12345  # time corrected by 0.012345 seconds
        try:
            self.jazz.addOaiRecord('43', sets=[('setSpec', 'setName')], metadataFormats=[('other', 'schema', 'namespace'), ('oai_dc','schema', 'namespace')])
            self.fail()
        except ValueError, e:
            self.assertTrue(str(e).startswith('Timestamp error: '), str(e))

        self.assertEquals(0, len(self.jazz._getSetList('setSpec')))
        self.assertEquals(0, len(self.jazz._getPrefixList('other')))
        self.assertEquals(1, len(self.jazz._getPrefixList('oai_dc')))

    def testFlattenSetHierarchy(self):
        self.assertEquals(['set1', 'set1:set2', 'set1:set2:set3'], sorted(_flattenSetHierarchy(['set1:set2:set3'])))
        self.assertEquals(['set1', 'set1:set2', 'set1:set2:set3', 'set1:set2:set4'], sorted(_flattenSetHierarchy(['set1:set2:set3', 'set1:set2:set4'])))

    def testGetUnique(self):
        newStamp = self.stampNumber
        self.jazz.addOaiRecord('id', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals(newStamp, self.jazz.getUnique('id'))

    def testWithObservablesAndUseOfAnyBreaksStuff(self):
        self.jazz.addOaiRecord('23', metadataFormats=[('one','schema1', 'namespace1'), ('two','schema2', 'namespace2')])
        server = be((Observable(),
            (Transparent(),
                (self.jazz,)
            )
        ))
        server.once.observer_init()
        mf = list(server.call.getAllMetadataFormats())
        self.assertEquals(2, len(mf))
        self.assertEquals(set(['one', 'two']), set(prefix for prefix, schema, namespace in mf))

    def testRecordId(self):
        r = RecordId('identifier', 12345)
        self.assertEquals(12345, r.stamp)
        self.assertEquals('identifier', r)
        r2 = r[6:]
        self.assertEquals('fier', r2)
        self.assertEquals(12345, r2.stamp)

    def testGetNrOfRecords(self):
        self.assertEquals(0, self.jazz.getNrOfRecords('aPrefix'))
        self.jazz.addOaiRecord('id1', metadataFormats=[('aPrefix', 'schema', 'namespace')])
        self.assertEquals(1, self.jazz.getNrOfRecords('aPrefix'))
        self.assertEquals(0, self.jazz.getNrOfRecords('anotherPrefix'))
        self.jazz.addOaiRecord('id2', metadataFormats=[('aPrefix', 'schema', 'namespace')])
        self.assertEquals(2, self.jazz.getNrOfRecords('aPrefix'))
        list(compose(self.jazz.delete('id1')))
        self.assertEquals(2, self.jazz.getNrOfRecords('aPrefix'))

    def testGetLastStampId(self):
        stampFunction = self.jazz._newStamp
        self.jazz = OaiJazz(self.tempdir, persistentDelete=False)
        self.jazz._newStamp = stampFunction
        self.assertEquals(None, self.jazz.getLastStampId('aPrefix'))
        newStamp = self.stampNumber
        self.jazz.addOaiRecord('id1', metadataFormats=[('aPrefix', 'schema', 'namespace')])
        self.assertEquals(newStamp, self.jazz.getLastStampId('aPrefix'))
        newStamp = self.stampNumber
        self.jazz.addOaiRecord('id2', metadataFormats=[('aPrefix', 'schema', 'namespace')])
        self.assertEquals(newStamp, self.jazz.getLastStampId('aPrefix'))
        self.jazz.purge('id2')
        self.jazz.purge('id1')
        self.assertEquals(None, self.jazz.getLastStampId('aPrefix'))

    def testIllegalSetRaisesException(self):
        # XSD: http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd
        # according to the xsd the setSpec should conform to:
        # ([A-Za-z0-9\-_\.!~\*'\(\)])+(:[A-Za-z0-9\-_\.!~\*'\(\)]+)*
        #
        # we will only check that a , (comma) is not used.
        self.assertEquals(',', SETSPEC_SEPARATOR)
        self.assertRaises(AssertionError, lambda: self.jazz.addOaiRecord('42', metadataFormats=[('prefix','schema', 'namespace')], sets=[('setSpec,', 'setName')]))

    def testConversionNeeded(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('prefix','schema', 'namespace')], sets=[('setSpec', 'setName')])
        remove(join(self.tempdir, 'identifier2setSpecs.bd'))
        self.assertRaises(AssertionError, lambda: OaiJazz(self.tempdir))

    def testVersionWritten(self):
        version = open(join(self.tempdir, "oai.version")).read()
        self.assertEquals(version, OaiJazz.version)
   
    def testRefuseInitWithNoVersionFile(self):
        self.oaiJazz = None
        remove(join(self.tempdir, 'oai.version'))

        try:
            oaiJazz = OaiJazz(self.tempdir)
            self.fail("Should have raised AssertionError with instruction of how to convert OAI index.")
        except AssertionError, e:
            self.assertEquals("The OAI index at %s need to be converted to the current version (with 'convert_oai_v2_to_v3.py' in meresco-oai/bin)" % self.tempdir, str(e))

    def testRefuseInitWithDifferentVersionFile(self):
        self.oaiJazz = None
        open(join(self.tempdir, 'oai.version'), 'w').write('different version')

        try:
            oaiJazz = OaiJazz(self.tempdir)
            self.fail("Should have raised AssertionError with instruction of how to convert OAI index.")
        except AssertionError, e:
            self.assertEquals("The OAI index at %s need to be converted to the current version (with 'convert_oai_v2_to_v3.py' in meresco-oai/bin)" % self.tempdir, str(e))

    def addDocuments(self, size):
        for id in range(1,size+1):
            self._addRecord(id)

    def _addRecord(self, anId):
        self.jazz.addOaiRecord('%05d' % anId, metadataFormats=[('oai_dc', 'dc.xsd', 'oai_dc.example.org')])

    def testAddDocument(self):
        self.addDocuments(1)
        result = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(['00001'], list(result))

    def testListRecords(self):
        #BooleanQuery.setMaxClauseCount(10) # Cause an early TooManyClauses exception.
        self.addDocuments(50)
        result = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals('00001', result.next())
        result = self.jazz.oaiSelect(prefix='oai_dc', continueAfter=str(self.jazz.getUnique('00001')))
        self.assertEquals('00002', result.next())

    def testChangedIdentifiersInOaiSelect(self):
        for i in range(1,5):
            self._addRecord(i)
        result = []
        select = self.jazz.oaiSelect(prefix='oai_dc')
        for r in select:
            result.append(r)
        self.assertEquals(['00001', '00002', '00003', '00004'], result)
        
        result = []
        select = self.jazz.oaiSelect(prefix='oai_dc')
        result.append(select.next())
        result.append(select.next())
        self._addRecord(3)
        for r in select:
            result.append(r)

        self.assertEquals(['00001', '00002', '00004'], result)


        result = []
        select = self.jazz.oaiSelect(prefix='oai_dc')
        for r in select:
            result.append(r)
        self.assertEquals(['00001', '00002', '00004', '00003'], result)

    def testAddOaiRecordWithNoMetadataFormats(self):
        try:
            self.jazz.addOaiRecord('identifier', sets=[('setSpec', 'setName')], metadataFormats=[])
            self.fail()
        except Exception, e:
            self.assertTrue('metadataFormat' in str(e))

    def testGetFromMultipleSets(self):
        self.jazz.addOaiRecord('id1', sets=[('set1', 'set1name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2', sets=[('set2', 'set2name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id3', sets=[('set3', 'set3name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['id1','id2'], list(self.jazz.oaiSelect(sets=['set1','set2'], prefix='prefix')))

    def testSetsMask(self):
        self.jazz.addOaiRecord('id1', sets=[('set1', 'set1name'), ('set3', 'set3name'), ('set4', 'set4name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2', sets=[('set2', 'set2name'), ('set3', 'set3name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2.1', sets=[('set2', 'set2name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['id1', 'id2'], list(self.jazz.oaiSelect(sets=['set1', 'set2'], prefix='prefix', setsMask=set(['set3']))))
        self.assertEquals(['id1'], list(self.jazz.oaiSelect(sets=['set1', 'set2'], prefix='prefix', setsMask=set(['set3', 'set4']))))


    def testListRecordsNoResults(self):
        result = self.jazz.oaiSelect(prefix='xxx')
        self.assertEquals([], list(result))
    
    def testAddSetInfo(self):
        header = '<header xmlns="http://www.openarchives.org/OAI/2.0/"><setSpec>%s</setSpec></header>'
        list(compose(self.oaiAddRecord.add(identifier='123', partname='oai_dc', lxmlNode=parseLxml(header % 1))))
        list(compose(self.oaiAddRecord.add(identifier='124', partname='oai_dc', lxmlNode=parseLxml(header % 2))))
        results = self.jazz.oaiSelect(sets=['1'], prefix='oai_dc')
        self.assertEquals(1, len(list(results)))
        results = self.jazz.oaiSelect(sets=['2'], prefix='oai_dc')
        self.assertEquals(1, len(list(results)))
        results = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(2, len(list(results)))

    def testGetAndAllSets(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix1', 'schema', 'namespace')], sets=[('setSpec1', 'setName1')])
        self.assertEquals(set(['setSpec1']), set(self.jazz.getSets('id:1')))
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix1', 'schema', 'namespace')], sets=[('setSpec1', 'setName1'), ('setSpec2:setSpec3', 'setName23')])
        self.assertEquals(set(['setSpec1']), set(self.jazz.getSets('id:1')))
        self.assertEquals(set(['setSpec1', 'setSpec2', 'setSpec2:setSpec3']), set(self.jazz.getSets('id:2')))
        self.assertEquals(set([]), set(self.jazz.getSets('doesNotExist')))
        self.assertEquals(set(['setSpec1', 'setSpec2', 'setSpec2:setSpec3']), set(self.jazz.getAllSets()))

    def testHierarchicalSets(self):
        self.jazz.addOaiRecord('record123', metadataFormats=[('oai_dc', 'schema', 'namespace')], sets=[('set1:set2:set3', 'setName123')])
        self.jazz.addOaiRecord('record124', metadataFormats=[('oai_dc', 'schema', 'namespace')], sets=[('set1:set2:set4', 'setName124')])
        
        self.assertEquals(['record123', 'record124'], list(self.jazz.oaiSelect(prefix='oai_dc', sets=['set1'])))
        self.assertEquals(['record123', 'record124'], list(self.jazz.oaiSelect(prefix='oai_dc', sets=['set1:set2'])))
        self.assertEquals(['record123'], list(self.jazz.oaiSelect(prefix='oai_dc', sets=['set1:set2:set3'])))

    def testAddOaiRecordPrefixOnly(self):
        self.jazz.addOaiRecord(identifier='oai://1234?34', sets=[], metadataFormats=[('prefix', 'schema', 'namespace')])
        
        recordIds = self.jazz.oaiSelect(prefix='prefix')
        self.assertEquals(['oai://1234?34'], list(recordIds))

    def testAddOaiRecord(self):
        self.jazz.addOaiRecord('identifier', sets=[('setSpec', 'setName')], metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['identifier'], list(self.jazz.oaiSelect(prefix='prefix')))
        self.assertEquals(['identifier'], list(self.jazz.oaiSelect(sets=['setSpec'],prefix='prefix')))
        self.assertEquals([], list(self.jazz.oaiSelect(sets=['unknown'],prefix='prefix')))
    
    def testAddOaiRecordWithNoSets(self):
        self.jazz.addOaiRecord('id1', sets=[], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2', sets=[], metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['id1', 'id2'], list(self.jazz.oaiSelect(prefix='prefix')))

    def testUpdateOaiRecord(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        result = self.jazz.oaiSelect(prefix='prefix')
        self.assertEquals(['id:1'],list(result))

    def testUpdateOaiRecordSet(self):
        self.jazz.addOaiRecord('id:1', sets=[('setSpec1', 'setName1')], metadataFormats=[('prefix', 'schema', 'namespace')])
        
        result = self.jazz.oaiSelect(prefix='prefix', sets=['setSpec1'])
        self.assertEquals(1, len(list(result)))

        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        
        result = self.jazz.oaiSelect(prefix='prefix')
        self.assertEquals(['id:1'],list(result))
        
        result = self.jazz.oaiSelect(prefix='prefix', sets=['setSpec1'])
        self.assertEquals(['id:1'], list(result))

    def testAddPartWithUniqueNumbersAndSorting(self):
        list(compose(self.oaiAddRecord.add(identifier='123', partname='oai_dc', lxmlNode=parseLxml('<oai_dc/>'))))
        list(compose(self.oaiAddRecord.add(identifier='124', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        list(compose(self.oaiAddRecord.add(identifier='121', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        list(compose(self.oaiAddRecord.add(identifier='122', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        results = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(1, len(list(results)))
        results = self.jazz.oaiSelect(prefix='lom')
        self.assertEquals(['124', '121','122'], list(results))
    
    def testAddOaiRecordWithUniqueNumbersAndSorting(self):
        self.jazz.addOaiRecord('123', metadataFormats=[('oai_dc', 'schema', 'namespace')])
        self.jazz.addOaiRecord('124', metadataFormats=[('lom', 'schema', 'namespace')])
        self.jazz.addOaiRecord('121', metadataFormats=[('lom', 'schema', 'namespace')])
        self.jazz.addOaiRecord('122', metadataFormats=[('lom', 'schema', 'namespace')])
        results = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(['123'], list(results))
        results =self.jazz.oaiSelect(prefix='lom')
        self.assertEquals(['124', '121','122'], list(results))

    def testGetDatestampNotExisting(self):
        self.assertEquals(None, self.jazz.getDatestamp('doesNotExist'))

    def testDelete(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        self.assertFalse(self.jazz.isDeleted('42'))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='oai_dc')))
        list(compose(self.jazz.delete('42')))
        self.assertTrue(self.jazz.isDeleted('42'))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='oai_dc')))

    def testDeleteKeepsSetsAndPrefixes(self):
        self.jazz.addOaiRecord('42', sets=[('setSpec1', 'setName1'),('setSpec2', 'setName2')], metadataFormats=[('prefix1','schema', 'namespace'), ('prefix2','schema', 'namespace')])
        list(compose(self.jazz.delete('42')))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='prefix1')))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='prefix2')))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='prefix1', sets=['setSpec1'])))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='prefix1', sets=['setSpec2'])))
        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='prefix2', sets=['setSpec2'])))
        self.assertTrue(self.jazz.isDeleted('42'))
    
    def testDeleteAndReadd(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        list(compose(self.jazz.delete('42')))
        self.assertTrue(self.jazz.isDeleted('42'))
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        self.assertFalse(self.jazz.isDeleted('42'))

        self.assertEquals(['42'], list(self.jazz.oaiSelect(prefix='oai_dc')))
        
    def testListRecordsWithFromAndUntil(self):
        def setTime(year, month, day):
            self.jazz._newStamp = lambda: int(timegm((year, month, day, 0, 1, 0, 0, 0 ,0))*1000000.0)
        setTime(2007, 9, 21)
        self.jazz.addOaiRecord('4', metadataFormats=[('prefix','schema', 'namespace')])
        setTime(2007, 9, 22)
        self.jazz.addOaiRecord('3', metadataFormats=[('prefix','schema', 'namespace')])
        setTime(2007, 9, 23)
        self.jazz.addOaiRecord('2', metadataFormats=[('prefix','schema', 'namespace')])
        setTime(2007, 9, 24)
        self.jazz.addOaiRecord('1', metadataFormats=[('prefix','schema', 'namespace')])
        
        result = self.jazz.oaiSelect(prefix='prefix', oaiFrom="2007-09-22T00:00:00Z")
        self.assertEquals(3, len(list(result)))
        result = self.jazz.oaiSelect(prefix='prefix', oaiFrom="2007-09-22T00:00:00Z", oaiUntil="2007-09-23T23:59:59Z")
        self.assertEquals(2, len(list(result)))

    def testOaiSelectWithContinuAt(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix', 'schema', 'namespace')])
        
        continueAfter = str(self.jazz.getUnique('id:1'))
        self.assertEquals(['id:2'], list(self.jazz.oaiSelect(prefix='prefix', continueAfter=continueAfter)))

        #add again will change the unique value
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals(['id:2', 'id:1'], list(self.jazz.oaiSelect(prefix='prefix', continueAfter=continueAfter)))
        
    def testGetAllMetadataFormats(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals([('prefix', 'schema', 'namespace')], list(self.jazz.getAllMetadataFormats()))
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix2', 'schema2', 'namespace2')])
        self.assertEquals(set([('prefix', 'schema', 'namespace'), ('prefix2', 'schema2', 'namespace2')]), set(self.jazz.getAllMetadataFormats()))

    def testGetAndAllPrefixes(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix1', 'schema', 'namespace')])
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix1', 'schema', 'namespace'), ('prefix2', 'schema', 'namespace')])
        self.assertEquals(set(['prefix1', 'prefix2']), set(self.jazz.getAllPrefixes()))
        self.assertEquals(set(['prefix1']), set(self.jazz.getPrefixes('id:1')))
        self.assertEquals(set(['prefix1', 'prefix2']) , set(self.jazz.getPrefixes('id:2')))
        self.assertEquals(set([]), set(self.jazz.getPrefixes('doesNotExist')))

    def testPreserveRicherPrefixInfo(self):
        list(compose(self.oaiAddRecord.add(identifier='457', partname='oai_dc', lxmlNode=parseLxml('<oai_dc:dc xmlns:oai_dc="http://oai_dc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
             xsi:schemaLocation="http://oai_dc http://oai_dc/dc.xsd"/>'))))
        list(compose(self.oaiAddRecord.add(identifier='457', partname='oai_dc', lxmlNode=parseLxml('<oai_dc/>'))))
        metadataFormats = set(self.jazz.getAllMetadataFormats())
        self.assertEquals(set([('oai_dc', 'http://oai_dc/dc.xsd', 'http://oai_dc')]), metadataFormats)

    def testIncompletePrefixInfo(self):
        list(compose(self.oaiAddRecord.add(identifier='457', partname='dc2', lxmlNode=parseLxml('<oai_dc/>'))))
        metadataFormats = set(self.jazz.getAllMetadataFormats())
        self.assertEquals(set([('dc2', '', '')]), metadataFormats)

    def testMetadataPrefixesOnly(self):
        list(compose(self.oaiAddRecord.add(identifier='456', partname='oai_dc', lxmlNode=parseLxml('<oai_dc:dc xmlns:oai_dc="http://oai_dc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
             xsi:schemaLocation="http://oai_dc http://oai_dc/dc.xsd"/>'))))
        prefixes = set(self.jazz.getAllPrefixes())
        self.assertEquals(set(['oai_dc']), prefixes)
        list(compose(self.oaiAddRecord.add(identifier='457', partname='dc2', lxmlNode=parseLxml('<oai_dc:dc xmlns:oai_dc="http://dc2"/>'))))
        prefixes = set(self.jazz.getAllPrefixes())
        self.assertEquals(set(['oai_dc', 'dc2']), prefixes)
        
    def testGetPrefixes(self):
        list(compose(self.oaiAddRecord.add(identifier='123', partname='oai_dc', lxmlNode=parseLxml('<dc/>'))))
        list(compose(self.oaiAddRecord.add(identifier='123', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        parts = set(self.jazz.getPrefixes('123'))
        self.assertEquals(set(['oai_dc', 'lom']), parts)
        self.assertEquals(['123'], list(self.jazz.oaiSelect(prefix='lom')))
        self.assertEquals(['123'], list(self.jazz.oaiSelect(prefix='oai_dc')))

    def testAddSuspendedListRecord(self):
        suspend = self.jazz.suspend(clientIdentifier="a-client-id").next()
        self.assertTrue({'a-client-id': suspend}, self.jazz._suspended)
        self.assertEquals(Suspend, type(suspend))

    def testSuspendSameClientTwiceBeforeResuming(self):
        reactor = CallTrace("reactor")
        resumed = []
        
        suspendGen1 = self.jazz.suspend(clientIdentifier="a-client-id")
        suspend1 = suspendGen1.next()
        suspend1(reactor, lambda: resumed.append(True))
        suspend2 = self.jazz.suspend(clientIdentifier="a-client-id").next()
        
        try:
            suspendGen1.next()
            self.fail()
        except ValueError, e:
            self.assertTrue([True], resumed)
            self.assertEquals("Aborting suspended request because of new request for the same OaiClient with identifier: a-client-id.", str(e))

    def testShouldResumeAPreviousSuspendAfterTooManySuspends(self):
        reactor = CallTrace("reactor")
        resumed = []
        jazz = OaiJazz(self.tempdir, maximumSuspendedConnections=1)
        suspendGen1 = jazz.suspend(clientIdentifier="a-client-id")
        suspend1 = suspendGen1.next()
        suspend1(reactor, lambda: resumed.append(True))
        with stderr_replaced() as s:
            suspend2 = jazz.suspend(clientIdentifier="another-client-id").next()

        self.assertRaises(StopIteration, lambda: suspendGen1.next())
        self.assertTrue([True], resumed)
        self.assertEquals(1, len(jazz._suspended))
        self.assertEquals("Too many suspended connections in OaiJazz. One random connection has been resumed.\n", s.getvalue())

    def testAddOaiRecordResumes(self):
        reactor = CallTrace("reactor")
        suspend = self.jazz.suspend(clientIdentifier="a-client-id").next()
        resumed = []
        suspend(reactor, lambda: resumed.append(True))

        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix', 'schema', 'namespace')])

        self.assertEquals([True], resumed)
        self.assertEquals({}, self.jazz._suspended)

    def testDeleteResumes(self):
        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix', 'schema', 'namespace')])
        reactor = CallTrace("reactor")
        suspend = self.jazz.suspend(clientIdentifier="a-client-id").next()
        resumed = []
        suspend(reactor, lambda: resumed.append(True))

        list(compose(self.jazz.delete(identifier='identifier')))

        self.assertEquals([True], resumed)
        self.assertEquals({}, self.jazz._suspended)

    def testStamp2Zulutime(self):
        self.assertEquals("2012-10-04T09:21:04Z", stamp2zulutime("1349342464630008"))
        self.assertEquals("", stamp2zulutime(None))
        self.assertRaises(Exception, stamp2zulutime, "not-a-stamp")

    def testRecoverFromCrashingAddOaiRecord(self):
        identifier = 'oai://1234?34'
        def setUp(jazz):
            jazz.addOaiRecord(identifier=identifier, sets=[('A', 'set A')], metadataFormats=[('prefix', 'schema', 'namespace')])

        def modify(jazz):
            jazz.addOaiRecord(identifier=identifier, sets=[('B', 'set B')], metadataFormats=[('prefix2', 'schema2', 'namespace2')])

        def asserts(jazz):
            self.assertEquals(['A', 'B'], jazz.getSets(identifier))
            self.assertEquals(set(['prefix', 'prefix2']), set(jazz.getPrefixes(identifier)))

        self._crashAtDifferentSteps(setUp, modify, asserts)

    def testRecoverFromCrashingDelete(self):
        identifier = 'oai://1234?34'
        def setUp(jazz):
            jazz.addOaiRecord(identifier=identifier, sets=[('A', 'set A')], metadataFormats=[('prefix', 'schema', 'namespace')])

        def modify(jazz):
            list(compose(jazz.delete(identifier)))

        def asserts(jazz):
            self.assertEquals(['A'], jazz.getSets(identifier))
            self.assertEquals(set(['prefix']), set(jazz.getPrefixes(identifier)))
            self.assertTrue(jazz.isDeleted(identifier))

        self._crashAtDifferentSteps(setUp, modify, asserts)

    def testRecoverFromCrashingPurge(self):
        identifier = 'oai://1234?34'
        def setUp(jazz):
            jazz._persistentDelete = False
            jazz.addOaiRecord(identifier=identifier, sets=[('A', 'set A')], metadataFormats=[('prefix', 'schema', 'namespace')])

        def modify(jazz):
            jazz.purge(identifier)

        def asserts(jazz):
            self.assertEquals(None, jazz.getUnique(identifier))

        self._crashAtDifferentSteps(setUp, modify, asserts)
        
    def _crashAtDifferentSteps(self, setUp, modify, asserts):
        for crashingStep in range(1, 15):
            crashingJazz = OaiJazz(self.tempdir)
            setUp(crashingJazz)
            self.assertFalse(isfile(crashingJazz._changeFile))

            stepCount = [0]
            def replaceMethodWithCrashAtStep(object, methodName):
                original = getattr(object, methodName)
                def crashAtStep(*args, **kwargs):
                    stepCount[0] += 1
                    if stepCount[0] == crashingStep:
                        raise RuntimeError("crash in '%s' of %s@%s for args %s and kwargs %s" % (methodName, object.__class__.__name__, id(object), args, kwargs))
                    original(*args, **kwargs)
                setattr(object, methodName, crashAtStep)

            replaceMethodWithCrashAtStep(crashingJazz, '_removeIfInList')
            replaceMethodWithCrashAtStep(crashingJazz, '_appendIfNotYet')
            replaceMethodWithCrashAtStep(crashingJazz._stamp2identifier, 'sync')
            replaceMethodWithCrashAtStep(crashingJazz._identifier2setSpecs, 'sync')

            try:
                with stderr_replaced():
                    modify(crashingJazz)
                # not crashed...
                asserts(crashingJazz)
            except SystemExit:
                self.assertTrue(isfile(crashingJazz._changeFile))
                crashingJazz = None

            # recover
            newJazz = OaiJazz(aDirectory=self.tempdir)
            self.assertFalse(isfile(newJazz._changeFile))
            asserts(newJazz)


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
# Copyright (C) 2011-2017 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016-2017 SURFmarket https://surf.nl
# Copyright (C) 2017 SURF https://surf.nl
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

import sys
import warnings
from os import remove, makedirs, listdir
from os.path import join, isdir
from time import time, sleep
from traceback import print_exc
from calendar import timegm
from StringIO import StringIO

from lxml.etree import parse

from seecr.test import SeecrTestCase, CallTrace
from seecr.test.io import stderr_replaced, stdout_replaced

from weightless.core import be, compose, consume
from meresco.core import Observable, Transparent

from org.apache.lucene.document import Document, LongField, Field, NumericDocValuesField, StringField
from org.apache.lucene.index import Term

from meresco.oai import OaiJazz, OaiAddRecord, stamp2zulutime
import meresco.oai.oaijazz as jazzModule
from meresco.oai.oaijazz import SETSPEC_SEPARATOR, lazyImport, _setSpecAndSubsets
from meresco.oai._partition import Partition
lazyImport()

# Suppress DeprecationWarning for OaiJazz.addOaiRecord(); since this will be triggered by other Meresco Oai modules for the time being...
def _suppressByTriggeringWarnings():
    from tempfile import mkdtemp
    from shutil import rmtree
    tmpdir = mkdtemp()
    try:
        jazz = OaiJazz(tmpdir)
        jazz.addObserver(CallTrace())
        with warnings.catch_warnings(record=True) as warns:
            jazz.addOaiRecord('id:1', metadataFormats=[('prefix', '', '')])
            jazz.addOaiRecord('id:1', metadataPrefixes=['f'], sets=[('s', 'set s')])
            assert warns, 'Expected a warning to be triggered!'
    finally:
        jazz.close()
        rmtree(tmpdir)
_suppressByTriggeringWarnings()


class OaiJazzTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.jazz = OaiJazz(join(self.tempdir, "a"))
        self.observer = CallTrace()
        self.jazz.addObserver(self.observer)
        self._originalNewStamp = self.jazz._newStamp
        self.stampNumber = self.originalStampNumber = int((timegm((2008, 07, 06, 05, 04, 03, 0, 0, 1))+.123456)*1000000)
        def stamp():
            result = self.stampNumber
            self.stampNumber += 1
            return result
        self.jazz._newStamp = stamp
        self.oaiAddRecord = OaiAddRecord()
        self.oaiAddRecord.addObserver(self.jazz)

    def tearDown(self):
        if self.jazz:
            self.jazz.close()
        SeecrTestCase.tearDown(self)

    def tmpdir2(self, name):
        path = join(self.tempdir, name)
        if not isdir(path):
            makedirs(path)
        return path

    def testObservableName(self):
        jazz = OaiJazz(self.tmpdir2("b"), name='someName')
        observable = Observable()
        observable.addObserver(jazz)

        self.assertEquals('someName', jazz.observable_name())

        result = observable.call['someName'].getNrOfRecords()
        self.assertEquals({'total': 0, 'deletes': 0}, result)

    def testOriginalStamp(self):
        jazz = OaiJazz(self.tmpdir2("b"))
        stamp0 = jazz._newStamp()
        sleep(0.0001)
        stamp1 = jazz._newStamp()
        self.assertTrue(stamp0 < stamp1, "Expected %s < %s" % (stamp0, stamp1))

    def testResultsStored(self):
        self.jazz.addOaiRecord(identifier='oai://1234?34', sets=[], metadataFormats=[('prefix', 'schema', 'namespace')])
        self.jazz.close()
        myJazz = OaiJazz(self.tmpdir2("a"))
        result = myJazz.oaiSelect(prefix='prefix')
        self.assertEquals('oai://1234?34', result.records.next().identifier)

    def testAddOaiRecordEmptyIdentifier(self):
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord("", metadataFormats=[('prefix', 'schema', 'namespace')]))
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord(None, metadataFormats=[('prefix', 'schema', 'namespace')]))

    def testAddOaiRecordWithoutMetadataPrefixesOrFormats(self):
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord("id1"))
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord("id1", metadataPrefixes=None))
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord("id1", metadataPrefixes=[]))
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord("id1", metadataFormats=[]))

    def testIdentifierWithSpace(self):
        identifier = "a b"
        self.jazz.addOaiRecord(identifier=identifier, sets=[], metadataFormats=[('prefix', 'schema', 'namespace')])
        record = self.jazz.getRecord(identifier)
        self.assertEquals(('a b', False), (record.identifier, record.isDeleted))
        records = list(self.jazz.oaiSelect(prefix='prefix').records)
        self.assertEquals([('a b', False)], [(r.identifier, r.isDeleted) for r in records])
        list(self.jazz.delete(identifier))
        records = list(self.jazz.oaiSelect(prefix='prefix').records)
        self.assertEquals([('a b', True)], [(r.identifier, r.isDeleted) for r in records])
        record = self.jazz.getRecord(identifier)
        self.assertEquals(('a b', True), (record.identifier, record.isDeleted))

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
        OaiJazz(self.tmpdir2("b"))
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
        self.assertEquals('2008-07-06T05:04:03Z', self.jazz.getRecord('123').getDatestamp())

    def testDeleteNonExistingRecords(self):
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        list(compose(self.jazz.delete('notExisting')))
        self.assertEquals(None, self.jazz.getRecord('notExisting'))
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        self.assertEquals(None, jazz2.getRecord('notExisting'))

    def testDeleteOaiRecordWithEmptyIdentifierRaises(self):
        try:
            self.jazz.deleteOaiRecord(identifier='')
            self.fail()
        except ValueError, e:
            self.assertEquals('Empty identifier not allowed.', str(e))

    def testDeleteOaiRecordNonExistingRecords(self):
        self.jazz.deleteOaiRecord(identifier='notExisting')
        self.assertEquals(None, self.jazz.getRecord('notExisting'))
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        self.assertEquals(None, jazz2.getRecord('notExisting'))

    def testDeleteOaiRecordNonExistingRecordWithPrefix(self):
        self.jazz.deleteOaiRecord(identifier='notExisting', metadataPrefixes=['p'])
        record = self.jazz.getRecord('notExisting')
        self.assertEquals('notExisting', record.identifier)
        self.assertEquals(set(['p']), record.prefixes)
        self.assertEquals(True, record.isDeleted)
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        record = jazz2.getRecord('notExisting')
        self.assertEquals('notExisting', record.identifier)
        self.assertEquals(set(['p']), record.prefixes)
        self.assertEquals(True, record.isDeleted)
        self.assertEquals([('p', '', '')], list(self.jazz.getAllMetadataFormats()))

    def testDeleteOaiRecordNonExistingRecordWithPrefixAndAlwaysDeleteInPrefixes(self):
        jazz2 = OaiJazz(self.tmpdir2("b"), alwaysDeleteInPrefixes=['z'])
        jazz2.addObserver(self.observer)
        jazz2.deleteOaiRecord(identifier='notExisting', metadataPrefixes=['p'])
        record = jazz2.getRecord('notExisting')
        self.assertEquals('notExisting', record.identifier)
        self.assertEquals(set(['p', 'z']), record.prefixes)
        self.assertEquals(True, record.isDeleted)
        self.assertEquals(set([('p', '', ''), ('z', '', '')]), set(jazz2.getAllMetadataFormats()))

    def testDeleteOaiRecordNonExistingRecordWithSets(self):
        self.jazz.deleteOaiRecord(identifier='notExisting', metadataPrefixes=['p'], setSpecs=['s1', 's2'])
        record = self.jazz.getRecord('notExisting')
        self.assertEquals('notExisting', record.identifier)
        self.assertEquals(set(['s1', 's2']), record.sets)
        self.assertEquals(True, record.isDeleted)
        self.assertEquals([('s1', ''), ('s2', '')], list(self.jazz.getAllSets(includeSetNames=True)))

    def testDeleteOaiRecordUnknownRecordWithSetsWithoutPrefixes(self):
        self.assertRaises(ValueError, lambda: self.jazz.deleteOaiRecord(identifier='notExisting', setSpecs=['s1']))

    def testDeleteEmptyIdentifier(self):
        self.assertRaises(ValueError, lambda: list(compose(self.jazz.delete(""))))
        self.assertRaises(ValueError, lambda: list(compose(self.jazz.delete(None))))

    def testMarkDeleteOfNonExistingRecordInGivenPrefixes(self):
        jazz = OaiJazz(self.tmpdir2("b"), alwaysDeleteInPrefixes=["aprefix"])
        jazz.addObserver(self.observer)
        jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        list(compose(jazz.delete('notExisting')))
        self.assertEquals(['notExisting'], recordIds(jazz.oaiSelect(prefix='aprefix')))
        self.assertEquals(['existing'], recordIds(jazz.oaiSelect(prefix='prefix')))
        list(compose(jazz.delete('existing')))
        self.assertEquals(['notExisting', 'existing'], recordIds(jazz.oaiSelect(prefix='aprefix')))

    def testPurgeRecord(self):
        self.jazz = OaiJazz(self.tmpdir2("b"), persistentDelete=False)
        self.jazz.addObserver(self.observer)
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        self.assertNotEquals(None, self.jazz.getRecord('existing'))
        self.jazz.purge('existing')
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("b"))
        jazz2.addObserver(self.observer)
        self.assertEquals(None, jazz2.getRecord('existing'))
        self.assertEquals([], recordIds(jazz2.oaiSelect(prefix='prefix')))

    def testPurgeNotAllowedIfDeletesArePersistent(self):
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        try:
            self.jazz.purge('existing')
            self.fail("Should fail on purging because deletes are persistent")
        except KeyError, e:
            self.assertEquals("'Purging of records is not allowed with persistent deletes.'", str(e))

    def testPurgeOverrideIfNotAllowed(self):
        self.jazz.addOaiRecord('existing', metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.purge('existing', ignorePeristentDelete=True)
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        jazz2.addObserver(self.observer)
        self.assertEquals(None, jazz2.getRecord('existing'))
        self.assertEquals([], recordIds(jazz2.oaiSelect(prefix='prefix')))

    def testPurgeSet(self):
        self.jazz.updateMetadataFormat('prefix', 'schema', 'namespace')
        self.jazz.updateSet('a', 'set a')
        self.jazz.updateSet('b', 'set b')
        self.assertEquals(set(['a', 'b']), self.jazz.getAllSets())
        def add(identifier, setSpecs):
            self.jazz.addOaiRecord('id:%s' % identifier, metadataPrefixes=['prefix'], setSpecs=setSpecs)
        add('-', None)
        add('a', ['a'])
        add('b', ['b'])
        add('ab', ['a', 'b'])
        self.assertEquals(['id:-', 'id:a', 'id:b', 'id:ab'], recordIds(self.jazz.oaiSelect(prefix='prefix')))
        self.jazz.purgeFromSet('a', ignorePeristentDelete=True)
        self.assertEquals(['id:-', 'id:b'], recordIds(self.jazz.oaiSelect(prefix='prefix')))
        self.assertEquals(set(['b']), self.jazz.getAllSets())
        self.assertEquals(set(['b']), self.jazz.getRecord('id:b').sets)
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        self.assertEquals(['id:-', 'id:b'], recordIds(jazz2.oaiSelect(prefix='prefix')))
        self.assertEquals(set(['b']), jazz2.getAllSets())

    def testAddSetSpec(self):
        self.jazz.addOaiRecord('id', metadataPrefixes=['prefix'], setSpecs=['a'])
        self.assertEquals({'a'}, self.jazz.getRecord('id').sets)
        self.jazz.addOaiRecord('id', metadataPrefixes=['prefix'], setSpecs=['b'])
        self.assertEquals({'a', 'b'}, self.jazz.getRecord('id').sets)

    def testOverrideRecord(self):
        self.jazz.updateMetadataFormat('prefix', 'schema', 'namespace')
        self.jazz.updateMetadataFormat('prefix2', 'schema', 'namespace')
        self.jazz.updateSet('a', 'set a')
        self.jazz.updateSet('b', 'set b')
        self.jazz.addOaiRecord('id:ab', metadataPrefixes=['prefix'], setSpecs=['a', 'b'])
        self.assertEquals(set(['a', 'b']), self.jazz.getRecord('id:ab').sets)
        t0 = self.jazz.getRecord('id:ab').stamp
        self.jazz.overrideRecord(identifier='id:ab', metadataPrefixes=['prefix2'], setSpecs=['a'], ignoreOaiSpec=True)
        self.assertEquals(set(['a']), self.jazz.getRecord('id:ab').sets)
        self.assertEquals(set(['prefix2']), self.jazz.getRecord('id:ab').prefixes)
        self.assertTrue(t0 < self.jazz.getRecord('id:ab').stamp)
        self.jazz.close()

        jazz2 = OaiJazz(self.tmpdir2("a"))
        self.assertEquals(set(['a']), jazz2.getRecord('id:ab').sets)
        self.assertEquals(set(['prefix2']), jazz2.getRecord('id:ab').prefixes)

    def testOverrideDeletedRecord(self):
        self.jazz.updateMetadataFormat('prefix', 'schema', 'namespace')
        self.jazz.updateMetadataFormat('prefix2', 'schema', 'namespace')
        self.jazz.updateSet('a', 'set a')
        self.jazz.updateSet('b', 'set b')
        self.jazz.addOaiRecord('id:ab', metadataPrefixes=['prefix'], setSpecs=['a', 'b'])
        self.jazz.deleteOaiRecord(identifier='id:ab')
        self.assertEquals(set(['a', 'b']), self.jazz.getRecord('id:ab').sets)
        self.assertTrue(self.jazz.getRecord('id:ab').isDeleted)
        t0 = self.jazz.getRecord('id:ab').stamp
        self.jazz.overrideRecord(identifier='id:ab', metadataPrefixes=['prefix2'], setSpecs=['a'], ignoreOaiSpec=True)
        self.assertEquals(set(['a']), self.jazz.getRecord('id:ab').sets)
        self.assertEquals(set(['prefix2']), self.jazz.getRecord('id:ab').prefixes)
        self.assertTrue(t0 < self.jazz.getRecord('id:ab').stamp)
        self.assertTrue(self.jazz.getRecord('id:ab').isDeleted)
        self.jazz.close()



    def testOverrideRecordIgnoreOaiSpec(self):
        self.jazz.updateMetadataFormat('prefix', 'schema', 'namespace')
        self.jazz.updateSet('a', 'set a')
        self.jazz.updateSet('b', 'set b')
        self.jazz.addOaiRecord('id:ab', metadataPrefixes=['prefix'], setSpecs=['a', 'b'])
        self.assertRaises(KeyError, lambda: self.jazz.overrideRecord(identifier='id:ab', metadataPrefixes=['prefix2'], setSpecs=['a']))

    # What happens if you do addOaiRecord('id1', prefix='aap') and afterwards
    #   addOaiRecord('id1', prefix='noot')
    # According to the specification:
    # Deleted status is a property of individual records. Like a normal record, a deleted record is identified by a unique identifier, a metadataPrefix and a datestamp. Other records, with different metadataPrefix but the same unique identifier, may remain available for the item.

    def testDeleteIsPersistent(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        list(compose(self.jazz.delete('42')))
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='oai_dc')))
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        self.assertTrue(jazz2.getRecord('42').isDeleted)
        self.assertEquals(['42'], recordIds(jazz2.oaiSelect(prefix='oai_dc')))

    def testSelectOnlyBatchSize(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        self.jazz.addOaiRecord('43', metadataFormats=[('oai_dc','schema', 'namespace')])
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='oai_dc', batchSize=1)))

    def testAddOaiRecordPersistent(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('prefix','schema', 'namespace')], sets=[('setSpec', 'setName')])
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='prefix', sets=['setSpec'])))
        self.assertEquals(set([('setSpec', 'setName')]), self.jazz.getAllSets(includeSetNames=True))
        self.assertEquals([('prefix','schema', 'namespace')], list(self.jazz.getAllMetadataFormats()))
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        self.assertEquals(['42'], recordIds(jazz2.oaiSelect(prefix='prefix', sets=['setSpec'])))
        self.assertEquals(set([('setSpec', 'setName')]), jazz2.getAllSets(includeSetNames=True))
        self.assertEquals([('prefix','schema', 'namespace')], list(jazz2.getAllMetadataFormats()))

    def testUnicodeIdentifier(self):
        self.jazz.addOaiRecord(u'ë', metadataFormats=[('prefix','schema', 'namespace')], sets=[('setSpec', 'setName')])
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        jazz2.addObserver(self.observer)
        self.assertEquals(['ë'], recordIds(jazz2.oaiSelect(prefix='prefix', sets=['setSpec'])))
        self.assertFalse(jazz2.getRecord(u'ë').isDeleted)
        list(compose(jazz2.delete(u'ë')))
        self.assertTrue(jazz2.getRecord('ë').isDeleted)
        self.assertTrue(jazz2.getRecord(u'ë').isDeleted)
        self.assertNotEquals(None, jazz2.getRecord(u'ë').getDatestamp())
        self.assertNotEquals(None, jazz2.getRecord(u'ë'))

        jazz3 = OaiJazz(self.tmpdir2("b"), persistentDelete=False)
        jazz3.purge(u'ë')
        self.assertEquals([], recordIds(jazz3.oaiSelect(prefix='prefix', sets=['setSpec'])))

    def testWeirdSetOrPrefixNamesDoNotMatter(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('/%^!@#$   \n\t','schema', 'namespace')], sets=[('set%2Spec\n\n', 'setName')])
        self.jazz.close()
        jazz2 = OaiJazz(self.tmpdir2("a"))
        self.assertEquals(['42'], recordIds(jazz2.oaiSelect(prefix='/%^!@#$   \n\t', sets=['set%2Spec\n\n'])))

    def testOaiSelectWithFromAfterEndOfTime(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        result = self.jazz.oaiSelect(prefix='oai_dc', oaiFrom='9999-01-01T00:00:00Z')
        self.assertEquals(0, len(recordIds(result)))

    def testDeleteIncrementsDatestampAndUnique(self):
        self.jazz.addOaiRecord('23', metadataFormats=[('oai_dc','schema', 'namespace')])
        stamp = self.jazz.getRecord('23').getDatestamp()
        self.stampNumber += 1234567890 # increaseTime
        list(compose(self.jazz.delete('23')))
        self.assertNotEqual(stamp, self.jazz.getRecord('23').getDatestamp())

    def testTimeUpdateRaisesErrorButLeavesIndexCorrect(self):
        self.jazz._newStamp = self._originalNewStamp
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        self.jazz._newestStamp += 123456  # time corrected by 0.123456 seconds
        newestStamp = self.jazz._newestStamp
        self.jazz.addOaiRecord('43', sets=[('setSpec', 'setName')], metadataFormats=[('other', 'schema', 'namespace'), ('oai_dc','schema', 'namespace')])
        self.assertEqual(newestStamp + 1, self.jazz.getRecord('43').stamp)
        self.jazz.close()
        self.jazz = OaiJazz(self.tmpdir2("a"))
        self.assertEqual(newestStamp + 1, self.jazz._newestStamp)

    def testSetSpecAndSubsets(self):
        self.assertEquals(['aap'], list(_setSpecAndSubsets('aap')))
        self.assertEquals(['aap:noot', 'aap'], list(_setSpecAndSubsets('aap:noot')))
        self.assertEquals(['a:b:c', 'a:b', 'a'], list(_setSpecAndSubsets('a:b:c')))

    def testGetUnique(self):
        newStamp = self.stampNumber
        self.jazz.addOaiRecord('id', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals(newStamp, self.jazz.getRecord('id').stamp)

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

    def testRecord(self):
        self.jazz.addOaiRecord('id1', metadataFormats=[('aPrefix', 'schema', 'namespace')], sets=[('setSpec', 'setName')])
        r = self.jazz.getRecord('id1')
        self.assertEquals('id1', r.identifier)
        self.assertEquals(self.originalStampNumber, r.stamp)
        self.assertEquals(set(['setSpec']), r.sets)

    def testGetNrOfRecords(self):
        self.assertEquals({'total': 0, 'deletes': 0}, self.jazz.getNrOfRecords('aPrefix'))
        self.jazz.addOaiRecord('id1', metadataFormats=[('aPrefix', 'schema', 'namespace')])
        self.assertEquals({'total': 1, 'deletes': 0}, self.jazz.getNrOfRecords('aPrefix'))
        self.assertEquals({'total': 0, 'deletes': 0}, self.jazz.getNrOfRecords('anotherPrefix'))
        self.jazz.addOaiRecord('id2', metadataFormats=[('aPrefix', 'schema', 'namespace')])
        self.assertEquals({'total': 2, 'deletes': 0}, self.jazz.getNrOfRecords('aPrefix'))
        list(compose(self.jazz.delete('id1')))
        self.assertEquals({'total': 2, 'deletes': 1}, self.jazz.getNrOfRecords('aPrefix'))
        self.assertEquals({'deletes': 1, 'total': 2}, self.jazz.getNrOfRecords(prefix='aPrefix', continueAfter='0', oaiFrom='2008-07-06T00:00:00Z'))
        self.assertEquals({'deletes': 1, 'total': 2}, self.jazz.getNrOfRecords(prefix='aPrefix', oaiFrom='2008-07-06T00:00:00Z'))
        self.assertEquals({'deletes': 0, 'total': 1}, self.jazz.getNrOfRecords(prefix='aPrefix', partition=Partition.create('1/2')))
        self.assertEquals({'deletes': 1, 'total': 1}, self.jazz.getNrOfRecords(prefix='aPrefix', partition=Partition.create('2/2')))

    def testMoreRecordsAvailable(self):
        def reopen():
            "Reopen everytime to force merging and causing an edge case to happen."
            self.jazz.close()
            self.jazz = OaiJazz(join(self.tempdir, "a"))
            self.jazz.addObserver(self.observer)
        for i in range(1,6):
            self.jazz.addOaiRecord('id%s' % i, metadataFormats=[('aPrefix', 'schema', 'namespace')], sets=[('set1', 'setName')])
            reopen()
        for i in range(6,12):
            self.jazz.addOaiRecord('id%s' % i, metadataFormats=[('aPrefix', 'schema', 'namespace')], sets=[('set2', 'setName')])
            reopen()
        result = self.jazz.oaiSelect(prefix='aPrefix', sets=['set1'], batchSize=2)
        self.assertTrue(result.moreRecordsAvailable)

    def testGetLastStampId(self):
        stampFunction = self.jazz._newStamp
        self.jazz = OaiJazz(self.tmpdir2("b"), persistentDelete=False)
        self.jazz.addObserver(self.observer)
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
        self.assertRaises(ValueError, lambda: self.jazz.addOaiRecord('42', metadataFormats=[('prefix','schema', 'namespace')], sets=[('setSpec,', 'setName')]))

    def testVersionWritten(self):
        version = open(join(self.tmpdir2("a"), "oai.version")).read()
        self.assertEquals(version, OaiJazz.version)

    def testStoreSetName(self):
        self.jazz.addOaiRecord("id:1", metadataFormats=[('p', 'x', 'b')],
            sets=[("spec1", "name1")])
        sets = self.jazz.getAllSets(includeSetNames=True)
        self.assertEquals([("spec1", "name1")], list(sets))

        self.jazz.addOaiRecord("id:2", metadataFormats=[('p', 'x', 'b')],
            sets=[("spec2", "")])
        sets = self.jazz.getAllSets(includeSetNames=True)
        self.assertEquals(2, len(list(sets)))
        self.assertEquals(set([("spec1", "name1"), ('spec2', '')]), set(list(sets)))

    def testRefuseInitWithNoVersionFile(self):
        self.oaiJazz = None
        remove(join(self.tmpdir2("a"), 'oai.version'))

        try:
            OaiJazz(self.tmpdir2("a"))
            self.fail("Should have raised AssertionError with instruction of how to convert OAI index.")
        except AssertionError, e:
            self.assertEquals("The OAI index at %s need to be converted to the current version (with 'convert_oai_v8_to_v9' in meresco-oai/bin)" % self.tmpdir2("a"), str(e))

    @stdout_replaced
    def testRefuseInitWithDifferentVersionFile(self):
        self.jazz.handleShutdown()
        self.jazz = None
        open(join(self.tmpdir2("a"), 'oai.version'), 'w').write('different version')

        try:
            OaiJazz(self.tmpdir2("a"))
            self.fail("Should have raised AssertionError with instruction of how to convert OAI index.")
        except AssertionError, e:
            self.assertEquals("The OAI index at %s need to be converted to the current version (with 'convert_oai_v8_to_v9' in meresco-oai/bin)" % self.tmpdir2("a"), str(e))

    def addDocuments(self, size):
        for id in range(1,size+1):
            self._addRecord(id)

    def _addRecord(self, anId):
        self.jazz.addOaiRecord('%05d' % anId, metadataFormats=[('oai_dc', 'dc.xsd', 'oai_dc.example.org')])

    def testAddDocument(self):
        self.addDocuments(1)
        result = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(['00001'], recordIds(result))

    def testListRecords(self):
        self.addDocuments(50)
        result = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals('00001', result.records.next().identifier)
        result = self.jazz.oaiSelect(prefix='oai_dc', continueAfter=str(self.jazz.getRecord('00001').stamp))
        self.assertEquals('00002', result.records.next().identifier)

    def testChangedIdentifiersInOaiSelect(self):
        for i in range(1,7):
            self._addRecord(i)
        result = []
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=4)
        for r in oaiSelectResult.records:
            result.append(r)
        self.assertEquals(['00001', '00002', '00003', '00004'], [r.identifier for r in result])

        result = []
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=4)
        result.append(oaiSelectResult.records.next())
        result.append(oaiSelectResult.records.next())
        self._addRecord(3)
        for r in oaiSelectResult.records:
            result.append(r)
        self.assertEquals(['00001', '00002', '00004'], [r.identifier for r in result])
        result = []
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=4)
        for r in oaiSelectResult.records:
            result.append(r)
        self.assertEquals(['00001', '00002', '00004', '00005'], [r.identifier for r in result])

    def testBatchWithMoreRemaining(self):
        for i in range(7):
            self._addRecord(i+1)
        result = []
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=4, shouldCountHits=True)
        for r in oaiSelectResult.records:
            result.append(r)
        self.assertEquals(['00001', '00002', '00003', '00004'], [r.identifier for r in result])
        self.assertEquals(4, oaiSelectResult.numberOfRecordsInBatch)
        self.assertTrue(oaiSelectResult.moreRecordsAvailable)
        record4 = result[-1]
        self.assertEquals('00004', record4.identifier)
        self.assertEquals(record4.stamp, oaiSelectResult.continueAfter)
        self.assertEquals(3, oaiSelectResult.recordsRemaining)

    def testBatchWithMoreRemainingButNotCounting(self):
        for i in range(7):
            self._addRecord(i+1)
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=4, shouldCountHits=False)
        self.assertEquals(['00001', '00002', '00003', '00004'], [r.identifier for r in oaiSelectResult.records])
        self.assertEquals(4, oaiSelectResult.numberOfRecordsInBatch)
        self.assertTrue(oaiSelectResult.moreRecordsAvailable)
        self.assertFalse(hasattr(oaiSelectResult, 'recordsRemaining'))

    def testEmptyBatch(self):
        result = []
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=40, shouldCountHits=True)
        for r in oaiSelectResult.records:
            result.append(r)
        self.assertEquals([], [r.identifier for r in result])
        self.assertEquals(0, oaiSelectResult.numberOfRecordsInBatch)
        self.assertFalse(oaiSelectResult.moreRecordsAvailable)
        self.assertEquals(None, oaiSelectResult.continueAfter)
        self.assertEquals(0, oaiSelectResult.recordsRemaining)

    def testBatchWithLessRecordsThanBatchsize(self):
        for i in range(4):
            self._addRecord(i+1)
        result = []
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=40, shouldCountHits=True)
        for r in oaiSelectResult.records:
            result.append(r)
        self.assertEquals(['00001', '00002', '00003', '00004'], [r.identifier for r in result])
        self.assertEquals(4, oaiSelectResult.numberOfRecordsInBatch)
        self.assertFalse(oaiSelectResult.moreRecordsAvailable)
        record4 = result[-1]
        self.assertEquals('00004', record4.identifier)
        self.assertEquals(record4.stamp, oaiSelectResult.continueAfter)
        self.assertEquals(0, oaiSelectResult.recordsRemaining)

    def testBatchWithExactBatchsizeNrOfRecords(self):
        for i in range(4):
            self._addRecord(i+1)
        result = []
        oaiSelectResult = self.jazz.oaiSelect(prefix='oai_dc', batchSize=4, shouldCountHits=True)
        for r in oaiSelectResult.records:
            result.append(r)
        self.assertEquals(['00001', '00002', '00003', '00004'], [r.identifier for r in result])
        self.assertEquals(4, oaiSelectResult.numberOfRecordsInBatch)
        self.assertFalse(oaiSelectResult.moreRecordsAvailable)
        record4 = result[-1]
        self.assertEquals('00004', record4.identifier)
        self.assertEquals(record4.stamp, oaiSelectResult.continueAfter)
        self.assertEquals(0, oaiSelectResult.recordsRemaining)

    def testAddOaiRecordWithNoMetadataFormats(self):
        try:
            self.jazz.addOaiRecord('identifier', sets=[('setSpec', 'setName')], metadataFormats=[])
            self.fail()
        except Exception, e:
            self.assertTrue('No metadataPrefix specified' in str(e))

    def testGetFromMultipleSets(self):
        self.jazz.addOaiRecord('id1', sets=[('set1', 'set1name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2', sets=[('set2', 'set2name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id3', sets=[('set3', 'set3name')], metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['id1','id2'], recordIds(self.jazz.oaiSelect(sets=['set1','set2'], prefix='prefix')))

    def testSetsMask(self):
        self.jazz.addOaiRecord('id1', sets=[('set1', 'set1name'), ('set3', 'set3name'), ('set4', 'set4name')],
                                metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2', sets=[('set2', 'set2name'), ('set3', 'set3name')],
                                metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2.1', sets=[('set2', 'set2name')],
                                metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['id1', 'id2'],
            recordIds(self.jazz.oaiSelect(sets=['set1', 'set2'], prefix='prefix', setsMask=set(['set3']))))
        self.assertEquals(['id1'],
            recordIds(self.jazz.oaiSelect(sets=['set1', 'set2'], prefix='prefix', setsMask=set(['set3', 'set4']))))


    def testListRecordsNoResults(self):
        result = self.jazz.oaiSelect(prefix='xxx')
        self.assertEquals([], recordIds(result))

    def testAddSetInfo(self):
        header = '<header xmlns="http://www.openarchives.org/OAI/2.0/"><setSpec>%s</setSpec></header>'
        list(compose(self.oaiAddRecord.add(identifier='123', partname='oai_dc', lxmlNode=parseLxml(header % 1))))
        list(compose(self.oaiAddRecord.add(identifier='124', partname='oai_dc', lxmlNode=parseLxml(header % 2))))
        results = self.jazz.oaiSelect(sets=['1'], prefix='oai_dc')
        self.assertEquals(1, len(recordIds(results)))
        results = self.jazz.oaiSelect(sets=['2'], prefix='oai_dc')
        self.assertEquals(1, len(recordIds(results)))
        results = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(2, len(recordIds(results)))

    def testGetAndAllSets(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix1', 'schema', 'namespace')],
                    sets=[('setSpec1', 'setName1')])
        self.assertEquals(set(['setSpec1']), set(self.jazz.getRecord('id:1').sets))
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix1', 'schema', 'namespace')],
                    sets=[('setSpec1', 'setName1'), ('setSpec2:setSpec3', 'setName23')])
        self.assertEquals(set(['setSpec1']), set(self.jazz.getRecord('id:1').sets))
        self.assertEquals(set, type(self.jazz.getRecord('id:2').sets))
        self.assertEquals(set(['setSpec1', 'setSpec2', 'setSpec2:setSpec3']), self.jazz.getRecord('id:2').sets)

        self.assertEquals(set, type(self.jazz.getAllSets()))
        self.assertEquals(set(['setSpec1', 'setSpec2', 'setSpec2:setSpec3']), set(self.jazz.getAllSets()))

    def testHierarchicalSets(self):
        self.jazz.addOaiRecord('record123', metadataFormats=[('oai_dc', 'schema', 'namespace')],
                                sets=[('set1:set2:set3', 'setName123')])
        self.jazz.addOaiRecord('record124', metadataFormats=[('oai_dc', 'schema', 'namespace')],
                                sets=[('set1:set2:set4', 'setName124')])
        self.assertEquals(['record123', 'record124'], recordIds(self.jazz.oaiSelect(prefix='oai_dc', sets=['set1'])))
        self.assertEquals(['record123', 'record124'], recordIds(self.jazz.oaiSelect(prefix='oai_dc', sets=['set1:set2'])))
        self.assertEquals(['record123'], recordIds(self.jazz.oaiSelect(prefix='oai_dc', sets=['set1:set2:set3'])))
        expectedSets = set([
            'set1',
            'set1:set2',
            'set1:set2:set3',
            'set1:set2:set4',
        ])
        self.assertEquals(expectedSets, self.jazz.getAllSets())
        self.assertEquals(expectedSets, self.jazz.getAllSets(includeSetNames=False))
        self.assertEquals(set([
                ('set1', ''),
                ('set1:set2', ''),
                ('set1:set2:set3', 'setName123'),
                ('set1:set2:set4', 'setName124'),
            ]),
            self.jazz.getAllSets(includeSetNames=True)
        )

    def testHierarchicalSetsWithCorrectNames(self):
        self.jazz.addOaiRecord('r1', metadataFormats=[('oai_dc', 'schema', 'namespace')],
                                sets=[('set1:set2:set3', 'setName_1_2_3')])
        self.jazz.addOaiRecord('r2', metadataFormats=[('rdf', '', '')],
                                sets=[('set1:set2:set4', 'setName1_2_4')])
        self.jazz.addOaiRecord('r3', metadataFormats=[('rdf', '', '')],
                                sets=[('set1', 'setName1')])
        self.assertEquals(set([
                ('set1', 'setName1'),
                ('set1:set2', ''),
                ('set1:set2:set3', 'setName_1_2_3'),
                ('set1:set2:set4', 'setName1_2_4'),
            ]),
            self.jazz.getAllSets(includeSetNames=True)
        )

    def testUpdateSet(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('oai_dc', 'schema', 'namespace')], sets=[('aSet', '')])
        self.assertEquals(sorted([('aSet', '')]), sorted(self.jazz.getAllSets(includeSetNames=True)))

        self.jazz.updateSet(setSpec='aSet', setName='a set name')
        self.assertEquals(sorted([('aSet', 'a set name')]), sorted(self.jazz.getAllSets(includeSetNames=True)))

        self.jazz.updateSet(setSpec='newSet', setName='new set name')
        self.assertEquals(sorted([
                ('aSet', 'a set name'),
                ('newSet', 'new set name'),
            ]),
            sorted(self.jazz.getAllSets(includeSetNames=True)),
        )

    def testAddOaiRecordPrefixOnly(self):
        self.jazz.addOaiRecord(identifier='oai://1234?34', sets=[], metadataFormats=[('prefix', 'schema', 'namespace')])

        records = self.jazz.oaiSelect(prefix='prefix')
        self.assertEquals(['oai://1234?34'], recordIds(records))

    def testAddOaiRecord(self):
        self.jazz.addOaiRecord('identifier', sets=[('setSpec', 'setName')], metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['identifier'], recordIds(self.jazz.oaiSelect(prefix='prefix')))
        self.assertEquals(['identifier'], recordIds(self.jazz.oaiSelect(sets=['setSpec'],prefix='prefix')))
        self.assertEquals([], recordIds(self.jazz.oaiSelect(sets=['unknown'],prefix='prefix')))

    def testAddOaiRecordWithNoSets(self):
        self.jazz.addOaiRecord('id1', sets=[], metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.addOaiRecord('id2', sets=[], metadataFormats=[('prefix','schema', 'namespace')])
        self.assertEquals(['id1', 'id2'], recordIds(self.jazz.oaiSelect(prefix='prefix')))

    def testUpdateOaiRecord(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        result = self.jazz.oaiSelect(prefix='prefix')
        self.assertEquals(['id:1'],recordIds(result))
        self.assertEquals(['prefix'], sorted(self.jazz.getRecord('id:1').prefixes))

    def testUpdateOaiRecordSet(self):
        self.jazz.addOaiRecord('id:1', sets=[('setSpec1', 'setName1')],
                                metadataFormats=[('prefix', 'schema', 'namespace')])

        result = self.jazz.oaiSelect(prefix='prefix', sets=['setSpec1'])
        self.assertEquals(1, len(recordIds(result)))

        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])

        result = self.jazz.oaiSelect(prefix='prefix')
        self.assertEquals(['id:1'],recordIds(result))

        result = self.jazz.oaiSelect(prefix='prefix', sets=['setSpec1'])
        self.assertEquals(['id:1'], recordIds(result))

        self.jazz.addOaiRecord('id:1', sets=[('setSpec1', 'setName1')],
                                metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals(['setSpec1'], sorted(self.jazz.getRecord('id:1').sets))

        self.jazz.addOaiRecord('id:1', sets=[('setSpec2', 'setName2')],
                                metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals(['setSpec1', 'setSpec2'], sorted(self.jazz.getRecord('id:1').sets))

    def testAddPartWithUniqueNumbersAndSorting(self):
        list(compose(self.oaiAddRecord.add(identifier='123', partname='oai_dc', lxmlNode=parseLxml('<oai_dc/>'))))
        list(compose(self.oaiAddRecord.add(identifier='124', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        list(compose(self.oaiAddRecord.add(identifier='121', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        list(compose(self.oaiAddRecord.add(identifier='122', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        results = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(1, len(recordIds(results)))
        results = self.jazz.oaiSelect(prefix='lom')
        self.assertEquals(['124', '121','122'], recordIds(results))

    def testSortingOnNumericStampValue(self):
        for i in range(1000):
            self.jazz.addOaiRecord(str(i), metadataFormats=[('oai_dc', 'schema', 'namespace')])
        self.jazz.commit()

        doc = Document()
        doc.add(StringField("identifier", "5", Field.Store.YES))
        doc.add(LongField("stamp", long(1215320643123455), Field.Store.YES))
        doc.add(NumericDocValuesField("numeric_stamp", long(1215320643123455)))
        doc.add(StringField("prefix", "oai_dc", Field.Store.YES))
        self.jazz._writer.updateDocument(Term("identifier", "5"), doc)
        self.jazz._latestModifications.add(str("5"))

        result = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals('5', recordIds(result)[0])

    def testAddOaiRecordWithUniqueNumbersAndSorting(self):
        self.jazz.addOaiRecord('123', metadataFormats=[('oai_dc', 'schema', 'namespace')])
        self.jazz.addOaiRecord('124', metadataFormats=[('lom', 'schema', 'namespace')])
        self.jazz.addOaiRecord('121', metadataFormats=[('lom', 'schema', 'namespace')])
        self.jazz.addOaiRecord('122', metadataFormats=[('lom', 'schema', 'namespace')])
        results = self.jazz.oaiSelect(prefix='oai_dc')
        self.assertEquals(['123'], recordIds(results))
        results =self.jazz.oaiSelect(prefix='lom')
        self.assertEquals(['124', '121','122'], recordIds(results))

    def testDelete(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        r = self.jazz.getRecord('42')
        self.assertFalse(self.jazz.getRecord('42').isDeleted)
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='oai_dc')))
        list(compose(self.jazz.delete('42')))
        self.assertTrue(self.jazz.getRecord('42').isDeleted)
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='oai_dc')))

    def testDeleteKeepsSetsAndPrefixes(self):
        self.jazz.addOaiRecord('42', sets=[('setSpec1', 'setName1'),('setSpec2', 'setName2')], metadataFormats=[('prefix1','schema', 'namespace'), ('prefix2','schema', 'namespace')])
        list(compose(self.jazz.delete('42')))
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='prefix1')))
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='prefix2')))
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='prefix1', sets=['setSpec1'])))
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='prefix1', sets=['setSpec2'])))
        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='prefix2', sets=['setSpec2'])))
        self.assertTrue(self.jazz.getRecord('42').isDeleted)

    def testDeleteAndReadd(self):
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        list(compose(self.jazz.delete('42')))
        self.assertTrue(self.jazz.getRecord('42').isDeleted)
        self.jazz.addOaiRecord('42', metadataFormats=[('oai_dc','schema', 'namespace')])
        self.assertFalse(self.jazz.getRecord('42').isDeleted)

        self.assertEquals(['42'], recordIds(self.jazz.oaiSelect(prefix='oai_dc')))

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
        self.assertEquals(3, len(recordIds(result)))
        result = self.jazz.oaiSelect(prefix='prefix', oaiFrom="2007-09-22T00:00:00Z", oaiUntil="2007-09-23T23:59:59Z")
        self.assertEquals(2, len(recordIds(result)))

    def testOaiSelectWithContinuAt(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix', 'schema', 'namespace')])

        continueAfter = str(self.jazz.getRecord('id:1').stamp)
        self.assertEquals(['id:2'], recordIds(self.jazz.oaiSelect(prefix='prefix', continueAfter=continueAfter)))

        #add again will change the unique value
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals(['id:2', 'id:1'], recordIds(self.jazz.oaiSelect(prefix='prefix', continueAfter=continueAfter)))

    def testGetAllMetadataFormats(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals([('prefix', 'schema', 'namespace')], list(self.jazz.getAllMetadataFormats()))
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix2', 'schema2', 'namespace2')])
        self.assertEquals(set([('prefix', 'schema', 'namespace'), ('prefix2', 'schema2', 'namespace2')]), set(self.jazz.getAllMetadataFormats()))

    def testGetAllMetadataFormatsNewSchemaNsForPrefixOverwrites(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals([('prefix', 'schema', 'namespace')], list(self.jazz.getAllMetadataFormats()))
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix', 'NewSchema', 'NewNamespace')])
        self.assertEquals(set([('prefix', 'NewSchema', 'NewNamespace')]), set(self.jazz.getAllMetadataFormats()))

    def testGetAndAllPrefixes(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix1', 'schema', 'namespace')])
        self.jazz.addOaiRecord('id:2', metadataFormats=[('prefix1', 'schema', 'namespace'), ('prefix2', 'schema', 'namespace')])
        self.assertEquals(set(['prefix1', 'prefix2']), set(self.jazz.getAllPrefixes()))
        self.assertEquals(set(['prefix1']), set(self.jazz.getRecord('id:1').prefixes))
        self.assertEquals(set(['prefix1', 'prefix2']) , set(self.jazz.getRecord('id:2').prefixes))

    def testIsKnownPrefix(self):
        self.assertFalse(self.jazz.isKnownPrefix('prefix1'))
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix1', 'schema', 'namespace')])
        self.assertTrue(self.jazz.isKnownPrefix('prefix1'))

    def testUpdateMetadataFormat(self):
        self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', '', '')])
        self.assertEquals(sorted([('prefix', '', '')]), sorted(self.jazz.getAllMetadataFormats()))

        self.jazz.updateMetadataFormat(prefix='prefix', schema='schema.xsd', namespace='space:name')
        self.assertEquals(sorted([('prefix', 'schema.xsd', 'space:name')]), sorted(self.jazz.getAllMetadataFormats()))

        self.jazz.updateMetadataFormat(prefix='newfix', schema='s.xsd', namespace='s:n')
        self.assertEquals(sorted([
                ('prefix', 'schema.xsd', 'space:name'),
                ('newfix', 's.xsd', 's:n'),
            ]),
            sorted(self.jazz.getAllMetadataFormats()),
        )

    def testAddOaiRecordWithSetSpecsAndMetadataPrefixesIdenticalToDeprecatedArguments(self):
        def prefixesAndSets(record):
            return {'prefixes': set(record.prefixes), 'sets': set(record.sets)}

        self.jazz.addOaiRecord('id:1', metadataPrefixes=['p1'], setSpecs=['s1'])
        self.jazz.addOaiRecord('id:2', metadataPrefixes=['p1', 'p2'], setSpecs=['s1', 's2'])
        self.jazz.addOaiRecord('id:3', metadataPrefixes=['p2'], setSpecs=['s2'])

        self.assertEquals(sorted([('p1', '', ''), ('p2', '', '')]), sorted(self.jazz.getAllMetadataFormats()))
        self.assertEquals(sorted(['s1', 's2']), sorted(self.jazz.getAllSets()))
        self.assertEquals(sorted([('s1', ''), ('s2', '')]), sorted(self.jazz.getAllSets(includeSetNames=True)))
        self.assertEquals({'prefixes': set(['p1']), 'sets': set(['s1'])}, prefixesAndSets(self.jazz.getRecord('id:1')))
        self.assertEquals({'prefixes': set(['p1', 'p2']), 'sets': set(['s1', 's2'])}, prefixesAndSets(self.jazz.getRecord('id:2')))
        self.assertEquals({'prefixes': set(['p2']), 'sets': set(['s2'])}, prefixesAndSets(self.jazz.getRecord('id:3')))

        self.jazz.updateMetadataFormat(prefix='p1', schema='schema.xsd', namespace='space:name')
        self.jazz.updateSet(setSpec='s1', setName='set now named')

        self.assertEquals(sorted([('p1', 'schema.xsd', 'space:name'), ('p2', '', '')]), sorted(self.jazz.getAllMetadataFormats()))
        self.assertEquals(sorted(['s1', 's2']), sorted(self.jazz.getAllSets()))
        self.assertEquals(sorted([('s1', 'set now named'), ('s2', '')]), sorted(self.jazz.getAllSets(includeSetNames=True)))

    def testAddOaiRecordWithMetadataPrefixesAfterUpdateMetadataFormatDoesNotOverwrite(self):
        self.jazz.addOaiRecord('id:1', metadataPrefixes=['p1'], setSpecs=['s1'])
        self.assertEquals(sorted([('p1', '', '')]), sorted(self.jazz.getAllMetadataFormats()))  # Placeholders / emtpy string defaults.
        self.jazz.updateMetadataFormat(prefix='p1', schema='schema.xsd', namespace='space:name')
        self.jazz.addOaiRecord('id:1', metadataPrefixes=['p1'], setSpecs=['s2'])
        self.assertEquals(sorted([('p1', 'schema.xsd', 'space:name')]), sorted(self.jazz.getAllMetadataFormats()))

    def testAddOaiRecordWithMixedSetsAndSetSpecsNotAllowed(self):
        try:
            self.jazz.addOaiRecord('id:1', metadataFormats=[('whatever', '', '')], sets=[('setSpec1', 'setName1')], setSpecs=['setSpec2'])
            self.fail()
        except ValueError, e:
            self.assertEquals('Either use setSpecs or sets, not both.', str(e))

    def testAddOaiRecordWithMixedMetadataFormatsAndMetadataPrefixesNotAllowed(self):
        try:
            self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix1', 'schema1', 'ns1')], metadataPrefixes=['prefix2'])
            self.fail()
        except ValueError, e:
            self.assertEquals('Either use metadataPrefixes or metadataFormats, not both.', str(e))

    def testWarnWhenUsingSetsOrMetadataFormatsArgsInOaiAddRecord(self):
        with ensureModulesWarningsWithoutSideEffects(modules=[jazzModule], simplefilter="default"):
            with stderr_replaced() as s:
                self.jazz.addOaiRecord('id:1', metadataFormats=[('prefix', '', '')])
                result = s.getvalue()
                self.assertTrue('DeprecationWarning: Use updateMetadataFormat to register a schema and namespace with a metadataPrefix.' in result, result)

        with ensureModulesWarningsWithoutSideEffects(modules=[jazzModule], simplefilter="default", record=True) as warns:
            self.jazz.addOaiRecord('id:1', metadataPrefixes=['f'], sets=[('s', 'set s')])
            self.assertEquals(1, len(warns))
            self.assertEquals(DeprecationWarning, warns[0].category)
            self.assertEquals('Use updateSet to register a set name with a setSpec.', str(warns[0].message))

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
        prefixes = self.jazz.getAllPrefixes()
        self.assertEquals(set, type(prefixes))
        self.assertEquals(set(['oai_dc', 'dc2']), set(prefixes))

    def testGetPrefixes(self):
        list(compose(self.oaiAddRecord.add(identifier='123', partname='oai_dc', lxmlNode=parseLxml('<dc/>'))))
        list(compose(self.oaiAddRecord.add(identifier='123', partname='lom', lxmlNode=parseLxml('<lom/>'))))
        parts = self.jazz.getRecord('123').prefixes
        self.assertEquals(set, type(parts))
        self.assertEquals(set(['oai_dc', 'lom']), parts)
        self.assertEquals(['123'], recordIds(self.jazz.oaiSelect(prefix='lom')))
        self.assertEquals(['123'], recordIds(self.jazz.oaiSelect(prefix='oai_dc')))

    def testAddOaiRecordResumes(self):
        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals(['signalOaiUpdate'], self.observer.calledMethodNames())
        self.assertEquals({'metadataPrefixes': set(['prefix']), 'sets': set(), 'stamp':self.originalStampNumber}, self.observer.calledMethods[0].kwargs)

    def testDeleteResumes(self):
        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix', 'schema', 'namespace')])
        self.observer.calledMethods.reset()
        consume(self.jazz.delete(identifier='identifier'))
        self.assertEquals(['signalOaiUpdate'], self.observer.calledMethodNames())
        self.assertEquals({'metadataPrefixes': set(['prefix']), 'sets': set(), 'stamp':self.originalStampNumber+1}, self.observer.calledMethods[0].kwargs)


    def testStamp2Zulutime(self):
        self.assertEquals("2012-10-04T09:21:04Z", stamp2zulutime("1349342464030008"))
        self.assertEquals("", stamp2zulutime(None))
        self.assertRaises(Exception, stamp2zulutime, "not-a-stamp")
        self.assertEquals("2012-10-04T09:21:04.030008Z", stamp2zulutime("1349342464030008", preciseDatestamp=True))

    def testOaiSelectIsAlwaysSortedOnStamp(self):
        self.jazz = OaiJazz(join(self.tempdir, "b"))
        self.jazz.addObserver(self.observer)
        for i in range(1000):
            self.jazz.addOaiRecord("%s" % i, metadataFormats=[('prefix', 'schema', 'namespace')])
        l = [r.stamp for r in self.jazz.oaiSelect(prefix='prefix', batchSize=2000).records]
        self.assertEquals(l, sorted(l))
        self.assertEquals(1000, len(l))

        self.jazz.commit()
        l = [r.stamp for r in self.jazz.oaiSelect(prefix='prefix', batchSize=2000).records]
        self.assertEquals(l, sorted(l))
        self.assertEquals(1000, len(l))

        self.jazz.addOaiRecord("a", metadataFormats=[('prefix', 'schema', 'namespace')])
        l = [r.stamp for r in self.jazz.oaiSelect(prefix='prefix', batchSize=2000).records]
        self.assertEquals(l, sorted(l))
        self.assertEquals(1001, len(l))

        self.jazz.addOaiRecord("b", metadataFormats=[('prefix', 'schema', 'namespace')])
        self.jazz.commit()
        l = [r.stamp for r in self.jazz.oaiSelect(prefix='prefix', batchSize=2000).records]
        self.assertEquals(l, sorted(l))
        self.assertEquals(1002, len(l))

        self.jazz._writer.close()

        self.jazz = OaiJazz(join(self.tempdir, "b"))
        l = [r.stamp for r in self.jazz.oaiSelect(prefix='prefix', batchSize=2000).records]
        self.assertEquals(l, sorted(l))
        self.assertEquals(1002, len(l))

    def xtestSelectPerformance(self):
        for i in xrange(1000):
            self.jazz.addOaiRecord('id%s' % i, metadataFormats=[('prefix','schema', 'namespace')])
            if i % 10 == 0:
                self.jazz.commit()
        # self.jazz.handleShutdown()
        self.jazz.commit()
        sleep(1)
        # jazz = OaiJazz(join(self.tempdir, "a"))
        jazz = self.jazz

        for i in xrange(10):
            records = list(jazz.oaiSelect(prefix='prefix', batchSize=200))
        print 'after warmup'
        COUNT = 30
        t0 = time()
        for i in xrange(COUNT):
            records = list(jazz.oaiSelect(prefix='prefix', batchSize=200))
        print 'Took: %s' % ((time() - t0))
        print 'Took per batch: %s' % ((time() - t0) / COUNT)
        self.assertEquals(200, len(records))
        print [r.identifier for r in records[:10]]
        # print [str(r.stamp) for r in records]

    def testReaderClosed(self):
        for i in xrange(1000):
            self.jazz.addOaiRecord('id%s' % i, metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.oaiSelect(prefix='prefix')
        reader0 = self.jazz._reader
        nfiles0 = len(listdir(self.jazz._directory))
        for i in xrange(1000):
            self.jazz.addOaiRecord('id%s' % i, metadataFormats=[('prefix','schema', 'namespace')])
        self.jazz.oaiSelect(prefix='prefix')
        reader1 = self.jazz._reader
        self.assertTrue(reader0 != reader1)
        self.jazz.oaiSelect(prefix='prefix')
        nfiles1 = len(listdir(self.jazz._directory))
        self.assertEquals(nfiles0, nfiles1)

    @stdout_replaced
    def testJazzWithShutdown(self):
        jazz = OaiJazz(self.tmpdir2("b"))
        jazz.addObserver(self.observer)
        jazz.addOaiRecord(identifier="identifier", sets=[('A', 'set A')], metadataFormats=[('prefix', 'schema', 'namespace')])
        list(compose(jazz.delete(identifier='identifier')))
        jazz.handleShutdown()
        jazz.close()
        jazz = OaiJazz(self.tmpdir2("b"))
        self.assertEquals(1, len(recordIds(jazz.oaiSelect(prefix='prefix'))))
        self.assertEquals(1, len(recordIds(jazz.oaiSelect(prefix='prefix', sets=['A']))))
        self.assertTrue(jazz.getRecord('identifier').isDeleted)

    def testJazzWithoutCommit(self):
        theDir = self.tmpdir2("b")
        jazz = OaiJazz(theDir)
        jazz.addObserver(self.observer)
        jazz.addOaiRecord(identifier="identifier", sets=[('A', 'set A')], metadataFormats=[('prefix', 'schema', 'namespace')])
        from meresco.oai.oaijazz import getReader
        try:
            getReader(theDir)
            self.fail()
        except Exception, e:
            self.assertTrue("no segments" in str(e), str(e))

    def testOaiSelectWithParts(self):
        for i in ['id:1', 'id:2', 'id:3']:
            self.jazz.addOaiRecord(i, metadataPrefixes=['prefix'])
        self.assertEquals(['id:2', 'id:3'],
                recordIds(self.jazz.oaiSelect(
                    prefix='prefix',
                    partition=Partition.create('1/2'))))
        self.assertEquals(['id:1'],
                recordIds(self.jazz.oaiSelect(
                    prefix='prefix',
                    partition=Partition.create('2/2'))))

    def testOaiSelectWithHackedPartition(self):
        for i in ['id:1', 'id:2', 'id:3']:
            self.jazz.addOaiRecord(i, metadataPrefixes=['prefix'])
        self.assertEquals([719,266,51], [Partition.hashId(i) for i in ['id:1', 'id:2', 'id:3']])
        partition = CallTrace(returnValues=dict(ranges=[(0,256), (512,1024)]))
        self.assertEquals(['id:1', 'id:3'],
                recordIds(self.jazz.oaiSelect(
                    prefix='prefix',
                    partition=partition)))

    def testOaiWithDeleteInSetsSupport(self):
        jazz = OaiJazz(join(self.tempdir, 'b'), deleteInSets=True)
        for i in ['id:1', 'id:2', 'id:3', 'id:4']:
            jazz.addOaiRecord(i, metadataPrefixes=['prefix'], setSpecs=['one', 'two'])
        ids = lambda rs:[r.identifier for r in rs]
        result = jazz.oaiSelect(prefix='prefix', sets={'two'})
        records = list(result.records)
        self.assertEqual(['id:1', 'id:2', 'id:3', 'id:4'], ids(records))
        jazz.deleteOaiRecordInSets('id:3', setSpecs={'two'})
        result = jazz.oaiSelect(prefix='prefix', sets={'two'})
        recordsTwo = list(result.records)
        recordsOne = list(jazz.oaiSelect(prefix='prefix', sets={'one'}).records)
        self.assertEqual(['id:1', 'id:2', 'id:4', 'id:3'], ids(recordsTwo))
        self.assertEqual(['id:1', 'id:2', 'id:4', 'id:3'], ids(recordsOne))
        self.assertEqual([False, False, False, False], [r.isDeleted for r in recordsOne])
        self.assertEqual([False, False, False, True], [r.isDeleted for r in recordsTwo])
        jazz.deleteOaiRecordInSets('id:3', setSpecs={'one'})
        recordsTwo = list(jazz.oaiSelect(prefix='prefix', sets={'two'}).records)
        recordsOne = list(jazz.oaiSelect(prefix='prefix', sets={'one'}).records)
        recordsAll = list(jazz.oaiSelect(prefix='prefix').records)
        self.assertEqual(['id:1', 'id:2', 'id:4', 'id:3'], ids(recordsTwo))
        self.assertEqual(['id:1', 'id:2', 'id:4', 'id:3'], ids(recordsOne))
        self.assertEqual(['id:1', 'id:2', 'id:4', 'id:3'], ids(recordsAll))
        self.assertEqual([False, False, False, True], [r.isDeleted for r in recordsOne])
        self.assertEqual([False, False, False, True], [r.isDeleted for r in recordsTwo])
        self.assertEqual([False, False, False, True], [r.isDeleted for r in recordsAll])
        jazz.addOaiRecord('id:3', metadataPrefixes=['prefix'], setSpecs=['one'])
        recordsTwo = list(jazz.oaiSelect(prefix='prefix', sets={'two'}).records)
        recordsOne = list(jazz.oaiSelect(prefix='prefix', sets={'one'}).records)
        self.assertEqual(['id:1', 'id:2', 'id:4', 'id:3'], ids(recordsTwo))
        self.assertEqual(['id:1', 'id:2', 'id:4', 'id:3'], ids(recordsOne))
        self.assertEqual([False, False, False, True], [r.isDeleted for r in recordsTwo])
        self.assertEqual([False, False, False, False], [r.isDeleted for r in recordsOne])

    def testDeleteRecordWithNewSet(self):
        jazz = OaiJazz(join(self.tempdir, 'b'), deleteInSets=True)
        jazz.addOaiRecord('id', metadataPrefixes=['prefix'], setSpecs=['one',])
        jazz.deleteOaiRecordInSets('id', setSpecs={'two'})
        record = list(jazz.oaiSelect(prefix='prefix').records)[0]
        self.assertEqual({'one', 'two'}, record.sets)
        self.assertEqual({'two'}, record.deletedSets)

    def testSetsStaysDeleted(self):
        jazz = OaiJazz(join(self.tempdir, 'b'), deleteInSets=True)
        jazz.addOaiRecord('id', metadataPrefixes=['prefix'], setSpecs=['a'])
        self.assertEquals({'a'}, jazz.getRecord('id').sets)
        jazz.addOaiRecord('id', metadataPrefixes=['prefix'], setSpecs=['b'])
        self.assertEquals({'a', 'b'}, jazz.getRecord('id').sets)
        self.assertEquals(set(), jazz.getRecord('id').deletedSets)
        jazz.deleteOaiRecordInSets('id', setSpecs=['b'])
        self.assertEquals({'a', 'b'}, jazz.getRecord('id').sets)
        self.assertEquals({'b'}, jazz.getRecord('id').deletedSets)
        jazz.addOaiRecord('id', metadataPrefixes=['prefix'], setSpecs=['a', 'c'])
        self.assertEquals({'a', 'b', 'c'}, jazz.getRecord('id').sets)
        self.assertEquals({'b'}, jazz.getRecord('id').deletedSets)
        jazz.addOaiRecord('id', metadataPrefixes=['prefix'], setSpecs=['b'])
        self.assertEquals(set(), jazz.getRecord('id').deletedSets)

    def testDeleteRecordInSetsIfIdentifierDidntExist(self):
        jazz = OaiJazz(join(self.tempdir, 'b'), deleteInSets=True)
        jazz.deleteOaiRecordInSets('id', setSpecs=['b'])

def recordIds(oaiSelectResult):
    return [record.identifier for record in oaiSelectResult.records]

def parseLxml(s):
    return parse(StringIO(s)).getroot()

class ensureModulesWarningsWithoutSideEffects(object):
    def __init__(self, modules, simplefilter=None, **catchWarningsKwargs):
        self._modules = modules
        self._registries = {}
        self._simplefilter = simplefilter
        self._catch_warnings = warnings.catch_warnings(**catchWarningsKwargs)

    def __enter__(self):
        for module in self._modules:
            registry = getattr(module, '__warningregistry__', None)
            self._registries[module] = registry
            if registry is not None:
                delattr(module, '__warningregistry__')

        result = self._catch_warnings.__enter__()

        if self._simplefilter:
            try:
                warnings.resetwarnings()
                warnings.simplefilter(self._simplefilter)
            except Exception:
                sys.stderr.write('Suppressed exception:\n')
                print_exc()

        return result

    def __exit__(self, exc_type, exc_value, traceback):
        self._catch_warnings.__exit__(exc_type, exc_value, traceback)

        for module, registry in self._registries.items():
            if registry is None:
                if hasattr(module, '__warningregistry__'):
                    delattr(module, '__warningregistry__')
            else:
                module.__warningregistry__ = registry
        return False

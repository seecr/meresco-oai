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
# Copyright (C) 2011-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from sys import maxint
from os.path import isdir, join, isfile
from os import makedirs, listdir, getenv
from time import time, strftime, gmtime, strptime
from calendar import timegm
from random import choice

from meresco.core import asyncreturn
from weightless.io import Suspend

from json import load, dump
from warnings import warn

def importVM():
    maxheap = getenv('PYLUCENE_MAXHEAP')
    if not maxheap:
        maxheap = '4g'
        warn("Using '4g' as maxheap for lucene.initVM(). To override use PYLUCENE_MAXHEAP environment variable.")
    from lucene import initVM, getVMEnv
    try:
        VM = initVM(maxheap=maxheap)#, vmargs='-agentlib:hprof=heap=sites')
    except ValueError:
        VM = getVMEnv()
    return VM

imported = False
def lazyImport():
    global imported
    if imported:
        return
    imported = True

    importVM()

    from java.lang import Long
    from java.io import File
    from org.apache.lucene.document import Document, StringField, Field, LongField
    from org.apache.lucene.search import IndexSearcher, TermQuery, BooleanQuery, NumericRangeQuery, MatchAllDocsQuery
    from org.apache.lucene.search import BooleanClause, TotalHitCountCollector, Sort, SortField
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.document import NumericDocValuesField, StoredField
    from org.apache.lucene.index.sorter import SortingMergePolicy, NumericDocValuesSorter
    from org.apache.lucene.util import BytesRef, Version
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer

    from meresco_oai import initVM
    OAI_VM = initVM()

    from org.meresco.oai import OaiSortingCollector

    globals().update(locals())

DEFAULT_BATCH_SIZE = 200

class OaiJazz(object):
    version = '6'

    def __init__(self, aDirectory, termNumerator=None, alwaysDeleteInPrefixes=None, preciseDatestamp=False, persistentDelete=True, maximumSuspendedConnections=100, name=None):
        lazyImport()
        self._directory = aDirectory
        if not isdir(aDirectory):
            makedirs(aDirectory)
        self._versionFormatCheck()
        self._deletePrefixes = alwaysDeleteInPrefixes or []
        self._preciseDatestamp = preciseDatestamp
        self._persistentDelete = persistentDelete
        self._maximumSuspendedConnections = maximumSuspendedConnections
        self._name = name
        self._suspended = {}
        self._load()
        self._writer, self._reader, self._searcher = getLucene(aDirectory)
        self._latestModifications = set()
        self._newestStamp = self._newestStampFromIndex()

    _sets = property(lambda self: self._data["sets"])
    _prefixes = property(lambda self: self._data["prefixes"])

    def oaiSelect(self,
            sets=None,
            prefix='oai_dc',
            continueAfter='0',
            oaiFrom=None,
            oaiUntil=None,
            setsMask=None,
            batchSize=DEFAULT_BATCH_SIZE,
            shouldCountHits=False):
        searcher = self._getSearcher()
        query = BooleanQuery()
        if oaiFrom or continueAfter or oaiUntil:
            start = max(int(continueAfter)+1, self._fromTime(oaiFrom))
            stop = self._untilTime(oaiUntil) or Long.MAX_VALUE
            fromRange = NumericRangeQuery.newLongRange("stamp", start, stop, True, True)
            query.add(fromRange, BooleanClause.Occur.MUST)
        query.add(TermQuery(Term("prefix", prefix)), BooleanClause.Occur.MUST)
        if sets:
            setQuery = BooleanQuery()
            for setSpec in sets:
                setQuery.add(TermQuery(Term("sets", setSpec)), BooleanClause.Occur.SHOULD)
            query.add(setQuery, BooleanClause.Occur.MUST)
        if setsMask:
            for set_ in setsMask:
                query.add(TermQuery(Term("sets", set_)), BooleanClause.Occur.MUST)

        collector = OaiSortingCollector(batchSize, shouldCountHits)
        searcher.search(query, None, collector)

        return self._OaiSelectResult(docs=collector.docs(searcher),
                collector=collector,
                parent=self,
            )

    class _OaiSelectResult(object):
        def __init__(inner, docs, collector, parent):
            inner.docs = docs
            inner.moreRecordsAvailable = collector.moreRecordsAvailable
            inner.recordsRemaining = collector.remainingRecords()
            inner.parent = parent
            inner.records = inner._records()
            inner.numberOfRecordsInBatch = len(docs)
            inner.continueAfter = None if len(docs) == 0 else inner._record(docs[-1]).stamp

        def _record(inner, doc):
            return Record(doc, inner.parent._preciseDatestamp)

        def _records(inner):
            for doc in inner.docs:
                record = inner._record(doc)
                if record.identifier not in inner.parent._latestModifications:
                    yield record

    def addOaiRecord(self, identifier, sets=None, metadataFormats=None):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        msg = 'No metadataFormat specified for record with identifier "%s"' % identifier
        assert [prefix for prefix, schema, namespace in metadataFormats], msg
        doc = self._getDocument(identifier)
        if not doc:
            doc = Document()
            doc.add(StringField("identifier", identifier, Field.Store.YES))
        else:
            doc.removeFields("tombstone")
            doc.removeFields("stamp")
        newStamp = self._newStamp()
        doc.add(LongField("stamp", long(newStamp), Field.Store.YES))
        doc.add(NumericDocValuesField("stamp", long(newStamp)))
        if metadataFormats:
            oldPrefixes = set(doc.getValues("prefix"))
            for prefix, schema, namespace in metadataFormats:
                self._prefixes[prefix] = (schema, namespace)
                if not prefix in oldPrefixes:
                    doc.add(StringField("prefix", prefix, Field.Store.YES))
        if sets:
            oldSets = set(doc.getValues('sets'))
            for setSpec, setName in sets:
                msg = 'SetSpec "%s" contains illegal characters' % setSpec
                assert SETSPEC_SEPARATOR not in setSpec, msg
                subsets = setSpec.split(":")
                while subsets:
                    fullSetSpec = ':'.join(subsets)
                    if setName:
                        self._sets[fullSetSpec] = setName
                    if not fullSetSpec in oldSets:
                        doc.add(StringField("sets", fullSetSpec, Field.Store.YES))
                    subsets.pop()
        self._writer.updateDocument(Term("identifier", identifier), doc)
        self._latestModifications.add(str(identifier))
        self._resume()

    @asyncreturn
    def delete(self, identifier):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        doc = self._getDocument(identifier)
        if doc:
            doc.removeFields("stamp")
        else:
            if not self._deletePrefixes:
                return
            doc = Document()
            doc.add(StringField("identifier", identifier, Field.Store.YES))
        for prefix in self._deletePrefixes:
            doc.add(StringField("prefix", prefix, Field.Store.YES))
        doc.add(StoredField("tombstone", BytesRef()))
        doc.add(NumericDocValuesField("tombstone", long(1)))
        newStamp = self._newStamp()
        doc.add(LongField("stamp", long(newStamp), Field.Store.YES))
        doc.add(NumericDocValuesField("stamp", long(newStamp)))
        self._writer.updateDocument(Term("identifier", identifier), doc)
        self._latestModifications.add(str(identifier))
        self._resume()

    def purge(self, identifier):
        if self._persistentDelete:
            raise KeyError("Purging of records is not allowed with persistent deletes.")
        self._latestModifications.add(str(identifier))
        return self._purge(identifier)

    def getAllMetadataFormats(self):
        for prefix, (schema, namespace) in self._prefixes.iteritems():
            yield (prefix, schema, namespace)

    def getAllPrefixes(self):
        return set(self._prefixes.keys())

    def getAllSets(self, includeSetNames=False):
        if includeSetNames:
            return set(self._sets.items())
        return set(self._sets.keys())

    def getNrOfRecords(self, prefix='oai_dc', setSpec=None):
        searcher = self._getSearcher()
        collector = TotalHitCountCollector()
        if prefix is None and setSpec is None:
            query = MatchAllDocsQuery()
        else:
            query = BooleanQuery()
            if prefix is not None:
                query.add(TermQuery(Term("prefix", prefix)), BooleanClause.Occur.MUST)
            if setSpec is not None:
                query.add(TermQuery(Term("sets", setSpec)), BooleanClause.Occur.MUST)
        searcher.search(query, collector)
        return collector.getTotalHits()

    def getRecord(self, identifier):
        doc = self._getDocument(identifier)
        if doc is None:
            return None
        return Record(doc, preciseDatestamp=self._preciseDatestamp)

    def getDeletedRecordType(self):
        return "persistent" if self._persistentDelete else "transient"

    def getLastStampId(self, prefix='oai_dc', setSpec=None):
        searcher = self._getSearcher()
        sort = Sort(SortField(None, SortField.Type.DOC, True))
        if prefix is None and setSpec is None:
            query = MatchAllDocsQuery()
        else:
            if prefix is None:
                query = TermQuery(Term("sets", setSpec))
            else:
                query = TermQuery(Term("prefix", prefix))
        results = searcher.search(query, 1, sort)
        if results.totalHits < 1:
            return None
        return _stampFromDocument(searcher.doc(results.scoreDocs[0].doc))

    def commit(self):
        self._save()
        self._writer.commit()

    def handleShutdown(self):
        print 'handle shutdown: saving OaiJazz %s' % self._directory
        from sys import stdout; stdout.flush()
        self.close()

    def close(self):
        self._save()
        self._writer.close()

    def observable_name(self):
        return self._name

    def suspend(self, clientIdentifier):
        suspend = Suspend()
        if clientIdentifier in self._suspended:
            self._suspended.pop(clientIdentifier).throw(exc_type=ValueError, exc_value=ValueError("Aborting suspended request because of new request for the same OaiClient with identifier: %s." % clientIdentifier), exc_traceback=None)
        if len(self._suspended) == self._maximumSuspendedConnections:
            self._suspended.pop(choice(self._suspended.keys())).throw(ForcedResumeException, ForcedResumeException("OAI x-wait connection has been forcefully resumed."), None)
            sys.stderr.write("Too many suspended connections in OaiJazz. One random connection has been resumed.\n")
        self._suspended[clientIdentifier] = suspend
        yield suspend
        suspend.getResult()

    def _versionFormatCheck(self):
        versionFile = join(self._directory, "oai.version")
        msg = "The OAI index at %s need to be converted to the current version (with 'convert_oai_v5_to_v6' in meresco-oai/bin)" % self._directory
        assert listdir(self._directory) == [] or isfile(versionFile) and open(versionFile).read() == self.version, msg
        with open(versionFile, 'w') as f:
            f.write(self.version)

    def _newestStampFromIndex(self):
        searcher = self._getSearcher()
        maxDoc = searcher.getIndexReader().maxDoc()
        if maxDoc < 1:
            return 0
        return int(searcher.doc(maxDoc - 1).getField("stamp").numericValue().longValue())

    def _getSearcher(self, identifier=None):
        modifications = len(self._latestModifications)
        if modifications == 0:
            return self._searcher
        if identifier and str(identifier) not in self._latestModifications and modifications < 10000:
            return self._searcher
        newreader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
        if newreader:
            self._reader = newreader
            self._searcher = IndexSearcher(newreader)
        self._latestModifications.clear()
        return self._searcher

    def _fromTime(self, oaiFrom):
        if not oaiFrom:
            return 0
        return self._timeToNumber(oaiFrom)

    def _untilTime(self, oaiUntil):
        if not oaiUntil:
            return None
        UNTIL_IS_INCLUSIVE = 1 # Add one second to 23:59:59
        return self._timeToNumber(oaiUntil) + UNTIL_IS_INCLUSIVE

    @staticmethod
    def _timeToNumber(time):
        try:
            return int(timegm(strptime(time, '%Y-%m-%dT%H:%M:%SZ')) * DATESTAMP_FACTOR)
        except (ValueError, OverflowError):
            return maxint * DATESTAMP_FACTOR

    def _getDocument(self, identifier):
        docId = self._getDocId(identifier)
        return self._getSearcher(identifier).doc(docId) if docId is not None else None

    def _getDocId(self, identifier):
        searcher = self._getSearcher(identifier)
        results = searcher.search(TermQuery(Term("identifier", identifier)), 1)
        if results.totalHits == 0:
            return None
        return results.scoreDocs[0].doc

    def _newStamp(self):
        """time in microseconds"""
        newStamp = int(time() * DATESTAMP_FACTOR)
        if newStamp <= self._newestStamp:
            newStamp = self._newestStamp + 1
        self._newestStamp = newStamp
        return newStamp

    def _resume(self):
        while len(self._suspended) > 0:
            clientId, suspend = self._suspended.popitem()
            suspend.resume()

    def _purge(self, identifier):
        self._writer.deleteDocuments(Term("identifier", identifier))

    def _getStamp(self, identifier):
        doc = self._getDocument(identifier)
        if doc is None:
            return None
        return _stampFromDocument(doc)

    def _save(self):
        dump(self._data, open(join(self._directory, "data.json"), "w"))

    def _load(self):
        path = join(self._directory, "data.json")
        if isfile(path):
            self._data = load(open(path))
        else:
            self._data = dict(prefixes={}, sets={})

# helper methods

def getReader(path):
    return DirectoryReader.open(FSDirectory.open(File(path)))

def getLucene(path):
    directory = FSDirectory.open(File(path))
    analyzer = WhitespaceAnalyzer(Version.LUCENE_43)
    config = IndexWriterConfig(Version.LUCENE_43, analyzer)
    mergePolicy = config.getMergePolicy()
    sortingMergePolicy = SortingMergePolicy(mergePolicy, NumericDocValuesSorter("stamp", True))
    config.setMergePolicy(sortingMergePolicy)
    writer = IndexWriter(directory, config)
    reader = writer.getReader()
    searcher = IndexSearcher(reader)
    return writer, reader, searcher


class Record(object):
    def __init__(self, doc, preciseDatestamp=False):
        self._doc = doc
        self._preciseDatestamp = preciseDatestamp

    @property
    def identifier(self):
        if not hasattr(self, '_identifier'):
            self._identifier = str(self._doc.getField("identifier").stringValue())
        return self._identifier

    @identifier.setter
    def identifier(self, value):
        self._identifier = value

    @property
    def stamp(self):
        if not hasattr(self, '_stamp'):
            self._stamp = _stampFromDocument(self._doc)
        return self._stamp

    @property
    def isDeleted(self):
        if not hasattr(self, 'tombstone'):
            self.tombstone = self._doc.getField("tombstone")
        return self.tombstone is not None

    @property
    def prefixes(self):
        if not hasattr(self, '_prefixes'):
            self._prefixes = set(self._doc.getValues('prefix'))
        return self._prefixes

    @property
    def sets(self):
        if not hasattr(self, '_sets'):
            self._sets = set(self._doc.getValues('sets'))
        return self._sets

    def getDatestamp(self):
        return _stamp2zulutime(stamp=self.stamp, preciseDatestamp=self._preciseDatestamp)

def _flattenSetHierarchy(sets):
    """"[1:2:3, 1:2:4] => [1, 1:2, 1:2:3, 1:2:4]"""
    result = set()
    for setSpec in sets:
        parts = setSpec.split(':')
        for i in range(1, len(parts) + 1):
            result.add(':'.join(parts[:i]))
    return result

def stamp2zulutime(stamp):
    if stamp is None:
        return ''
    return _stamp2zulutime(int(stamp))

def _stamp2zulutime(stamp, preciseDatestamp=False):
    microseconds = ".%s" % (stamp % DATESTAMP_FACTOR) if preciseDatestamp else ""
    return "%s%sZ" % (strftime('%Y-%m-%dT%H:%M:%S', gmtime(stamp / DATESTAMP_FACTOR)), microseconds)

def _stampFromDocument(doc):
    return int(doc.getField("stamp").numericValue().longValue())

class ForcedResumeException(Exception):
    pass

SETSPEC_SEPARATOR = ","
DATESTAMP_FACTOR = 1000000

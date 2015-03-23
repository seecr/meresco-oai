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
# Copyright (C) 2011-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
from os import makedirs, listdir
from itertools import chain
from time import time, strftime, gmtime, strptime
from calendar import timegm
from random import choice
from warnings import warn

from weightless.io import Suspend

from json import load, dump
from meresco.pylucene import getJVM

imported = False
Long = File = Document = StringField = Field = LongField = IntField = IndexSearcher = TermQuery = \
    BooleanQuery = NumericRangeQuery = MatchAllDocsQuery = BooleanClause = TotalHitCountCollector = \
    Sort = SortField = DirectoryReader = Term = IndexWriter = IndexWriterConfig = FSDirectory = \
    NumericDocValuesField = SortingMergePolicy = BytesRef = Version = WhitespaceAnalyzer = \
    OaiSortingCollector = None

def lazyImport():
    global imported
    if imported:
        return
    imported = True

    VM = getJVM()

    from java.lang import Long
    from java.io import File
    from org.apache.lucene.document import Document, StringField, Field, LongField, IntField
    from org.apache.lucene.search import IndexSearcher, TermQuery, BooleanQuery, NumericRangeQuery, MatchAllDocsQuery
    from org.apache.lucene.search import BooleanClause, TotalHitCountCollector, Sort, SortField
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.document import NumericDocValuesField
    from org.apache.lucene.index.sorter import SortingMergePolicy
    from org.apache.lucene.util import BytesRef, Version
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer

    from meresco_oai import initVM
    OAI_VM = initVM()

    from org.meresco.oai import OaiSortingCollector

    globals().update(locals())


DEFAULT_BATCH_SIZE = 200

class OaiJazz(object):
    version = '8'

    def __init__(self, aDirectory, termNumerator=None, alwaysDeleteInPrefixes=None, preciseDatestamp=False, persistentDelete=True, maximumSuspendedConnections=100, name=None):
        lazyImport()
        self._directory = aDirectory
        if not isdir(aDirectory):
            makedirs(aDirectory)
        self._versionFormatCheck()
        self._deletePrefixes = set(alwaysDeleteInPrefixes or [])
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
            continueAfter=None,
            oaiFrom=None,
            oaiUntil=None,
            setsMask=None,
            batchSize=DEFAULT_BATCH_SIZE,
            shouldCountHits=False):
        searcher = self._getSearcher()
        query = self._luceneQuery(prefix=prefix, sets=sets, continueAfter=continueAfter, oaiFrom=oaiFrom, oaiUntil=oaiUntil, setsMask=setsMask)
        collector = OaiSortingCollector(batchSize, shouldCountHits)
        searcher.search(query, None, collector)
        return self._OaiSelectResult(docs=collector.docs(searcher),
                collector=collector,
                parent=self,
            )

    def _luceneQuery(self, prefix, sets=None, continueAfter=None, oaiFrom=None, oaiUntil=None, setsMask=None):
        query = BooleanQuery()
        if oaiFrom or continueAfter or oaiUntil:
            start = max(int(continueAfter or '0') + 1, self._fromTime(oaiFrom))
            stop = self._untilTime(oaiUntil) or Long.MAX_VALUE
            fromRange = NumericRangeQuery.newLongRange(STAMP_FIELD, start, stop, True, True)
            query.add(fromRange, BooleanClause.Occur.MUST)
        if prefix:
            query.add(TermQuery(Term(PREFIX_FIELD, prefix)), BooleanClause.Occur.MUST)
        if sets:
            setQuery = BooleanQuery()
            for setSpec in sets:
                setQuery.add(TermQuery(Term(SETS_FIELD, setSpec)), BooleanClause.Occur.SHOULD)
            query.add(setQuery, BooleanClause.Occur.MUST)
        for set_ in setsMask or []:
            query.add(TermQuery(Term(SETS_FIELD, set_)), BooleanClause.Occur.MUST)
        if query.clauses().size() == 0:
            query.add(MatchAllDocsQuery(), BooleanClause.Occur.MUST)
        return query

    class _OaiSelectResult(object):
        def __init__(inner, docs, collector, parent):
            inner.docs = docs
            inner.moreRecordsAvailable = collector.moreRecordsAvailable
            recordsRemaining = collector.remainingRecords()
            if recordsRemaining != -1:
                inner.recordsRemaining = recordsRemaining
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

    def addOaiRecord(self, identifier, metadataPrefixes=None, setSpecs=None, metadataFormats=None, sets=None):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        if not metadataFormats and not metadataPrefixes:
            raise ValueError('No metadataFormat or metadataPrefix specified for record with identifier "%s"' % identifier)
        if metadataFormats:
            if metadataPrefixes:
                raise ValueError("Either use metadataPrefixes or metadataFormats, not both.")
            warn("Use metadataPrefixes with the updateMetadataFormat method to add prefix metadata.", DeprecationWarning)  # Since 2015-03-20 / version 5.12
            metadataPrefixes = [p for (p, _, _) in metadataFormats]
            for prefix, schema, namespace in metadataFormats:
                self._prefixes[prefix] = (schema, namespace)
        if sets:
            if setSpecs:
                raise ValueError("Either use setSpecs or sets, not both.")
            warn("Use setSpecs with the updateSet method to add set metadata.", DeprecationWarning)  # Since 2015-03-20 / version 5.12
            setSpecs = [s for (s, _) in sets]
            for setSpec, setName in sets:
                self._sets[setSpec] = setName
        self._updateOaiRecord(identifier=identifier, metadataPrefixes=metadataPrefixes, setSpecs=setSpecs)

    def delete(self, identifier):
        self.deleteOaiRecord(identifier=identifier)
        return
        yield

    def deleteOaiRecord(self, identifier, setSpecs=None, metadataPrefixes=None):
        "deleteOaiRecord's granularity is per unique identifier; not per identifier & metadataPrefix combination (as optionally allowed by the spec)."
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        metadataPrefixes = self._deletePrefixes.union(metadataPrefixes or [])
        oldDoc = self._getDocument(identifier)
        if oldDoc is None and not metadataPrefixes:
            if setSpecs:
                raise ValueError('setSpec not allowed for unknown record if no metadataPrefixes are provided')
            return
        self._updateOaiRecord(identifier=identifier, setSpecs=setSpecs, metadataPrefixes=metadataPrefixes, delete=True, oldDoc=oldDoc)

    def purge(self, identifier):
        if self._persistentDelete:
            raise KeyError("Purging of records is not allowed with persistent deletes.")
        self._latestModifications.add(str(identifier))
        return self._purge(identifier)

    def updateMetadataFormat(self, prefix, schema, namespace):
        self._prefixes[prefix] = (schema, namespace)

    def getAllMetadataFormats(self):
        for prefix, (schema, namespace) in self._prefixes.iteritems():
            yield (prefix, schema, namespace)

    def getAllPrefixes(self):
        return set(self._prefixes.keys())

    def updateSet(self, setSpec, setName):
        self._sets[setSpec] = setName

    def getAllSets(self, includeSetNames=False):
        if includeSetNames:
            return set(self._sets.items())
        return set(self._sets.keys())

    def getNrOfRecords(self, prefix='oai_dc', setSpec=None, continueAfter=None, oaiFrom=None, oaiUntil=None):
        searcher = self._getSearcher()
        totalCollector = TotalHitCountCollector()
        query = self._luceneQuery(prefix=prefix, sets=[setSpec] if setSpec else None, continueAfter=continueAfter, oaiFrom=oaiFrom, oaiUntil=oaiUntil)
        searcher.search(query, totalCollector)

        query.add(TermQuery(Term(TOMBSTONE_FIELD, TOMBSTONE_VALUE)), BooleanClause.Occur.MUST)
        deleteCollector = TotalHitCountCollector()
        searcher.search(query, deleteCollector)

        return {"total": totalCollector.getTotalHits(), "deletes": deleteCollector.getTotalHits()}

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
                query = TermQuery(Term(SETS_FIELD, setSpec))
            else:
                query = TermQuery(Term(PREFIX_FIELD, prefix))
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

    def suspend(self, clientIdentifier, metadataPrefix, set=None):
        suspend = Suspend()
        suspend.oaiListResumeMask = dict(metadataPrefix=metadataPrefix, set=set)
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
        return _stampFromDocument(searcher.doc(maxDoc - 1))

    def _getSearcher(self, identifier=None):
        modifications = len(self._latestModifications)
        if modifications == 0:
            return self._searcher
        if identifier and str(identifier) not in self._latestModifications and modifications < 10000:
            return self._searcher
        newreader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
        if newreader:
            self._reader.close()
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
        results = searcher.search(TermQuery(Term(IDENTIFIER_FIELD, identifier)), 1)
        if results.totalHits == 0:
            return None
        return results.scoreDocs[0].doc

    def _updateOaiRecord(self, identifier, setSpecs, metadataPrefixes, delete=False, oldDoc=None):
        oldDoc = oldDoc or self._getDocument(identifier)
        doc = self._getNewDocument(identifier, oldDoc=oldDoc)
        if delete:
            doc.add(StringField(TOMBSTONE_FIELD, TOMBSTONE_VALUE, Field.Store.YES))
        newStamp = self._newStamp()
        doc.add(LongField(STAMP_FIELD, long(newStamp), Field.Store.YES))
        doc.add(NumericDocValuesField(NUMERIC_STAMP_FIELD, long(newStamp)))

        allMetadataPrefixes = set(doc.getValues(PREFIX_FIELD))
        self._setMetadataPrefixes(doc=doc, metadataPrefixes=metadataPrefixes, allMetadataPrefixes=allMetadataPrefixes)

        allSets = set(doc.getValues(SETS_FIELD))
        self._setSets(doc=doc, setSpecs=setSpecs or [], allSets=allSets)

        self._writer.updateDocument(Term(IDENTIFIER_FIELD, identifier), doc)
        self._latestModifications.add(str(identifier))
        self._resume(metadataPrefixes=allMetadataPrefixes, sets=allSets)

    def _getNewDocument(self, identifier, oldDoc):
        doc = Document()
        doc.add(StringField(IDENTIFIER_FIELD, identifier, Field.Store.YES))
        if oldDoc is not None:
            for oldPrefix in oldDoc.getValues(PREFIX_FIELD):
                doc.add(StringField(PREFIX_FIELD, oldPrefix, Field.Store.YES))
            for oldSet in oldDoc.getValues(SETS_FIELD):
                doc.add(StringField(SETS_FIELD, oldSet, Field.Store.YES))
        return doc

    def _newStamp(self):
        """time in microseconds"""
        newStamp = int(time() * DATESTAMP_FACTOR)
        if newStamp <= self._newestStamp:
            newStamp = self._newestStamp + 1
        self._newestStamp = newStamp
        return newStamp

    def _setMetadataPrefixes(self, doc, metadataPrefixes, allMetadataPrefixes):
        for prefix in metadataPrefixes:
            if prefix not in allMetadataPrefixes:
                doc.add(StringField(PREFIX_FIELD, prefix, Field.Store.YES))
                self._prefixes.setdefault(prefix, ('', ''))
                allMetadataPrefixes.add(prefix)

    def _setSets(self, doc, setSpecs, allSets):
        for setSpec in setSpecs:
            if SETSPEC_SEPARATOR in setSpec:
                raise ValueError('SetSpec "%s" contains illegal characters' % setSpec)
            for innerSetSpec in _setSpecAndSubsets(setSpec):
                self._sets.setdefault(innerSetSpec, '')
                if not innerSetSpec in allSets:
                    doc.add(StringField(SETS_FIELD, innerSetSpec, Field.Store.YES))
                    allSets.add(innerSetSpec)

    def _resume(self, metadataPrefixes, sets):
        for clientId, suspend in self._suspended.items()[:]:
            if suspend.oaiListResumeMask['metadataPrefix'] in metadataPrefixes:
                setMask = suspend.oaiListResumeMask['set']
                if setMask and not setMask in sets:
                    continue
                del self._suspended[clientId]
                suspend.resume()

    def _purge(self, identifier):
        self._writer.deleteDocuments(Term(IDENTIFIER_FIELD, identifier))

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
    analyzer = WhitespaceAnalyzer()
    config = IndexWriterConfig(Version.LATEST, analyzer)
    mergePolicy = config.getMergePolicy()
    sortingMergePolicy = SortingMergePolicy(mergePolicy, Sort(SortField(NUMERIC_STAMP_FIELD, SortField.Type.LONG)))
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
            self._identifier = str(self._doc.getField(IDENTIFIER_FIELD).stringValue())
        return self._identifier

    @property
    def stamp(self):
        if not hasattr(self, '_stamp'):
            self._stamp = _stampFromDocument(self._doc)
        return self._stamp

    @property
    def isDeleted(self):
        if not hasattr(self, 'tombstone'):
            self.tombstone = self._doc.getField(TOMBSTONE_FIELD)
        return self.tombstone is not None

    @property
    def prefixes(self):
        if not hasattr(self, '_prefixes'):
            self._prefixes = set(self._doc.getValues(PREFIX_FIELD))
        return self._prefixes

    @property
    def sets(self):
        if not hasattr(self, '_sets'):
            self._sets = set(self._doc.getValues(SETS_FIELD))
        return self._sets

    def getDatestamp(self):
        return _stamp2zulutime(stamp=self.stamp, preciseDatestamp=self._preciseDatestamp)


def _setSpecAndSubsets(setSpec):
    subsets = setSpec.split(SETSPEC_HIERARCHY_SEPARATOR)
    for i in range(len(subsets), 0, -1):
        yield SETSPEC_HIERARCHY_SEPARATOR.join(subsets[0:i])

def stamp2zulutime(stamp):
    if stamp is None:
        return ''
    return _stamp2zulutime(int(stamp))

def _stamp2zulutime(stamp, preciseDatestamp=False):
    microseconds = ".%s" % (stamp % DATESTAMP_FACTOR) if preciseDatestamp else ""
    return "%s%sZ" % (strftime('%Y-%m-%dT%H:%M:%S', gmtime(stamp / DATESTAMP_FACTOR)), microseconds)

def _stampFromDocument(doc):
    return int(doc.getField(STAMP_FIELD).numericValue().longValue())

class ForcedResumeException(Exception):
    pass

SETSPEC_SEPARATOR = ","
SETSPEC_HIERARCHY_SEPARATOR = ":"
DATESTAMP_FACTOR = 1000000

PREFIX_FIELD = "prefix"
SETS_FIELD = "sets"
IDENTIFIER_FIELD = "identifier"
STAMP_FIELD = "stamp"
NUMERIC_STAMP_FIELD = "numeric_stamp"
TOMBSTONE_FIELD = "tombstone"
TOMBSTONE_VALUE = "T"

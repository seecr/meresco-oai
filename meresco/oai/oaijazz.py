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
# Copyright (C) 2010-2011, 2018, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2011-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014, 2019 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015-2017 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016-2017, 2019 SURFmarket https://surf.nl
# Copyright (C) 2017, 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
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

from os.path import isdir, join, isfile
from os import makedirs, listdir, rename
from warnings import warn

from json import load, dump, dumps, loads
from meresco.core import Observable
from meresco.oaicommon import timeToNumber, stamp2zulutime, timestamp, Partition

try:
    from java.lang import Long
    from java.nio.file import Paths
    from org.apache.lucene.document import Document, StringField, Field, StoredField, LongPoint, IntPoint
    from org.apache.lucene.search import IndexSearcher, TermQuery, BooleanQuery, MatchAllDocsQuery
    from org.apache.lucene.search import BooleanClause, TotalHitCountCollector, Sort, SortField
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.document import NumericDocValuesField
    from org.apache.lucene.util import BytesRef, Version
    from lucene import JArray
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer
    from org.meresco.oai import OaiSortingCollector
except ImportError:
    raise ImportError("initVM() not called: please add to your project: 'from lucene import initVM; initVM(); from meresco_oai import initVM; initVM()'")


DEFAULT_BATCH_SIZE = 200

class OaiJazz(Observable):
    version = '12'

    def __init__(self, aDirectory, alwaysDeleteInPrefixes=None, persistentDelete=True, name=None, **kwargs):
        Observable.__init__(self, name=name)
        self._directory = aDirectory
        if not isdir(aDirectory):
            makedirs(aDirectory)
        self._versionFormatCheck()
        self._deletePrefixes = set(alwaysDeleteInPrefixes or [])
        self._persistentDelete = persistentDelete
        self._load()
        self._writer, self._reader, self._searcher = getLucene(aDirectory)
        self._latestModifications = set()
        self._newestStamp = self._newestStampFromIndex()
        self._deleteInSetsSupport = False
        if kwargs.get('deleteInSets'):
            # Supporting deleting in sets is not OAI-PMH compatible
            self._deleteInSetsSupport = True
        self._importMode = kwargs.get('importMode', False) # import mode when reading an export

    _sets = property(lambda self: self._data["sets"])
    _prefixes = property(lambda self: self._data["prefixes"])

    def oaiSelect(self,
            sets=None,
            prefix='oai_dc',
            continueAfter=None,
            oaiFrom=None,
            oaiUntil=None,
            setsMask=None,
            batchSize=None,
            partition=None,
            shouldCountHits=False):
        batchSize = DEFAULT_BATCH_SIZE if batchSize is None else batchSize
        searcher = self._getSearcher()
        queryBuilder = self._luceneQueryBuilder(prefix=prefix, sets=sets, setsMask=setsMask, partition=partition)
        collector = self._search(queryBuilder.build(), continueAfter, oaiFrom, oaiUntil, batchSize, shouldCountHits)
        return self._OaiSelectResult(docs=collector.docs(searcher),
                collector=collector,
                parent=self,
                requestedSets=sets if self._deleteInSetsSupport else None,
                requestedPrefix=prefix,
            )

    def _search(self, query, continueAfter, oaiFrom, oaiUntil, batchSize, shouldCountHits):
        searcher = self._getSearcher()

        start = max(int(continueAfter or '0') + 1, self._fromTime(oaiFrom))
        stop = self._untilTime(oaiUntil) or Long.MAX_VALUE

        collector = OaiSortingCollector(batchSize, shouldCountHits, int(start), int(stop))
        searcher.search(query, collector)
        return collector

    def _luceneQueryBuilder(self, prefix, sets=None, setsMask=None, partition=None):
        numberOfClausesAdded = 0
        queryBuilder = BooleanQuery.Builder()
        if prefix:
            queryBuilder.add(TermQuery(Term(PREFIX_FIELD, prefix)), BooleanClause.Occur.MUST)
            numberOfClausesAdded += 1
        if sets:
            setQueryBuilder = BooleanQuery.Builder()
            for setSpec in sets:
                setQueryBuilder.add(TermQuery(Term(SETS_FIELD, setSpec)), BooleanClause.Occur.SHOULD)
            queryBuilder.add(setQueryBuilder.build(), BooleanClause.Occur.MUST)
            numberOfClausesAdded += 1
        for set_ in setsMask or []:
            queryBuilder.add(TermQuery(Term(SETS_FIELD, set_)), BooleanClause.Occur.MUST)
            numberOfClausesAdded += 1
        if partition:
            partitionQueries = []
            for start, stop in partition.ranges():
                partitionQueries.append(IntPoint.newRangeQuery(HASH_FIELD, start, stop - 1))
            if len(partitionQueries) == 1:
                pQuery = partitionQueries[0]
            else:
                pQueryBuilder = BooleanQuery.Builder()
                for q in partitionQueries:
                    pQueryBuilder.add(q, BooleanClause.Occur.SHOULD)
                pQuery = pQueryBuilder.build()
            queryBuilder.add(pQuery, BooleanClause.Occur.MUST)
            numberOfClausesAdded += 1
        if numberOfClausesAdded == 0:
            queryBuilder.add(MatchAllDocsQuery(), BooleanClause.Occur.MUST)
        return queryBuilder

    class _OaiSelectResult(object):
        def __init__(inner, docs, collector, parent, requestedSets, requestedPrefix):
            inner.docs = docs
            inner.moreRecordsAvailable = collector.moreRecordsAvailable
            recordsRemaining = collector.remainingRecords()
            if recordsRemaining != -1:
                inner.recordsRemaining = recordsRemaining
            inner.parent = parent
            inner.records = inner._records(requestedSets=requestedSets, requestedPrefix=requestedPrefix)
            inner.numberOfRecordsInBatch = len(docs)
            inner.continueAfter = None if len(docs) == 0 else inner._record(docs[-1]).stamp

        def _record(inner, doc, **kwargs):
            return Record(doc, **kwargs)

        def _records(inner, **recordKwargs):
            for doc in inner.docs:
                record = inner._record(doc, **recordKwargs)
                if record.identifier not in inner.parent._latestModifications:
                    yield record

    def addOaiRecord(self, identifier, metadataPrefixes=None, setSpecs=None):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        if not metadataPrefixes:
            raise ValueError('No metadataPrefix specified for record with identifier "%s"' % identifier)
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

    def deleteOaiRecordInPrefixes(self, identifier, metadataPrefixes):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        if not metadataPrefixes:
            raise ValueError("Empty metadataPrefixes not allowed.")
        self._updateOaiRecord(identifier=identifier, setSpecs=[], metadataPrefixes=[], deleteInPrefixes=metadataPrefixes, delete=False)

    def deleteOaiRecordInSets(self, identifier, setSpecs):
        if not self._deleteInSetsSupport:
            raise ValueError('Deleting in sets not supported.')
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        oldDoc = self._getDocument(identifier)
        if oldDoc is None:
            return
        self._updateOaiRecord(identifier=identifier, setSpecs=[], metadataPrefixes=[], delete=False, deleteInSets=setSpecs, oldDoc=oldDoc)

    def purge(self, identifier, ignorePeristentDelete=False):
        if self._persistentDelete and not ignorePeristentDelete:
            raise KeyError("Purging of records is not allowed with persistent deletes.")
        self._latestModifications.add(str(identifier))
        return self._purge(identifier)

    def purgeFromSet(self, setSpec, ignorePeristentDelete=False):
        if self._persistentDelete and not ignorePeristentDelete:
            raise KeyError("Purging of a set is not allowed with persistent deletes.")
        self._latestModifications.update([setSpec] * (_MAX_MODIFICATIONS + 1))
        self._sets.pop(setSpec, None)
        return self._purgeFromSet(setSpec)

    def overrideRecord(self, identifier, metadataPrefixes, setSpecs, ignoreOaiSpec=False):
        if not ignoreOaiSpec:
            raise KeyError("Ignoring OAI Specification, so please use 'ignoreOaiSpec=True'.")
        deleted = self.getRecord(identifier).isDeleted
        self.purge(identifier, ignorePeristentDelete=True)
        self._updateOaiRecord(identifier=identifier, metadataPrefixes=metadataPrefixes, setSpecs=setSpecs, delete=deleted)

    def updateMetadataFormat(self, prefix, schema, namespace):
        self._prefixes[prefix] = (schema, namespace)

    def getAllMetadataFormats(self):
        for prefix, (schema, namespace) in self._prefixes.items():
            yield (prefix, schema, namespace)

    def getAllPrefixes(self):
        return set(self._prefixes.keys())

    def isKnownPrefix(self, prefix):
        return prefix in self._prefixes

    def updateSet(self, setSpec, setName):
        self._sets[setSpec] = setName

    def getAllSets(self, includeSetNames=False):
        if includeSetNames:
            return set(self._sets.items())
        return set(self._sets.keys())

    def getNrOfRecords(self, prefix='oai_dc', setSpec=None, continueAfter=None, oaiFrom=None, oaiUntil=None, partition=None):
        queryBuilder = self._luceneQueryBuilder(prefix=prefix, sets=[setSpec] if setSpec else None, partition=partition)
        collector = self._search(queryBuilder.build(), continueAfter, oaiFrom, oaiUntil, batchSize=1, shouldCountHits=True)

        queryBuilder.add(TermQuery(Term(TOMBSTONE_FIELD, TOMBSTONE_VALUE)), BooleanClause.Occur.MUST)

        deleteCollector = self._search(queryBuilder.build(), continueAfter, oaiFrom, oaiUntil, batchSize=1, shouldCountHits=True)
        return {"total": collector.totalHits(), "deletes": deleteCollector.totalHits()}

    def getRecord(self, identifier, metadataPrefix=None):
        doc = self._getDocument(identifier)
        if doc is None:
            return None
        return Record(doc, requestedPrefix=metadataPrefix)

    def getDeletedRecordType(self):
        return "persistent" if self._persistentDelete else "transient"

    def getLastStampId(self, prefix='oai_dc', setSpec=None):
        searcher = self._getSearcher()
        sort = Sort(SortField(NUMERIC_STAMP_FIELD, SortField.Type.LONG, True))
        if prefix is None and setSpec is None:
            query = MatchAllDocsQuery()
        else:
            if prefix is None:
                query = TermQuery(Term(SETS_FIELD, setSpec))
            else:
                query = TermQuery(Term(PREFIX_FIELD, prefix))
        results = searcher.search(query, 1, sort)
        if results.totalHits.value < 1:
            return None
        return _stampFromDocument(searcher.doc(results.scoreDocs[0].doc))

    def commit(self):
        self._save()
        self._writer.commit()

    def handleShutdown(self):
        print('handle shutdown: saving OaiJazz %s' % self._directory)
        from sys import stdout; stdout.flush()
        self.close()

    def close(self):
        self._save()
        self._writer.close()

    def export(self, outputfile):
        meta = dict(export_version=1, sets={}, metadataPrefixes={})
        for setSpec, setName in self.getAllSets(includeSetNames=True):
            meta['sets'][setSpec] = {'setName': setName}
        for prefix, schema, namespace in self.getAllMetadataFormats():
            meta['metadataPrefixes'][prefix]={'schema':schema, 'namespace': namespace}
        with open(outputfile, 'w') as f:
            f.write('META:\n')
            f.write(dumps(meta, sort_keys=True))
            f.write('\nRECORDS:\n')
            result = self.oaiSelect(prefix=None)
            while result.continueAfter:
                for r in result.records:
                    f.write(dumps(r.asExportDict(), sort_keys=True))
                    f.write('\n')
                result = self.oaiSelect(prefix=None, continueAfter=result.continueAfter)

    @classmethod
    def importDump(cls, directory, dumpfile):
        jazz = cls(directory, deleteInSets=True, importMode=True)
        with open(dumpfile) as d:
            assert 'META:\n' == next(d)
            meta = loads(next(d).strip())
            assert meta['export_version'] == 1
            for setSpec, setDict in list(meta.get('sets', {}).items()):
                jazz.updateSet(setSpec=setSpec, setName=setDict.get('setName', ''))
            for prefix, metadataDict in list(meta.get('metadataPrefixes', {}).items()):
                jazz.updateMetadataFormat(prefix, schema=metadataDict.get('schema', ''), namespace=metadataDict.get('namespace', ''))
            assert 'RECORDS:\n' == next(d)
            for record in d:
                record = loads(record.strip())
                jazz._updateOaiRecord(
                        identifier=record['identifier'],
                        setSpecs=record['sets'],
                        metadataPrefixes=record['prefixes'],
                        delete=record.get('tombstone', False),
                        deleteInSets=record.get('deletedSets', []),
                        deleteInPrefixes=record.get('deletedPrefixes', []),
                        _overrideStamp=record['timestamp'])
            jazz.close()
            jazz = None

        return True


    def _versionFormatCheck(self):
        versionFile = join(self._directory, "oai.version")
        msg = "The OAI index at %s is not compatible with this version (no conversion script could be provided)." % self._directory
        versionInFile = None
        if isfile(versionFile):
            with open(versionFile) as fp:
                versionInFile = fp.read()
        assert listdir(self._directory) == [] or isfile(versionFile) and versionInFile == self.version, msg
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
        if identifier and str(identifier) not in self._latestModifications and modifications < _MAX_MODIFICATIONS:
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
        return timeToNumber(oaiFrom)

    def _untilTime(self, oaiUntil):
        if not oaiUntil:
            return None
        UNTIL_IS_INCLUSIVE = 1 # Add one second to 23:59:59
        return timeToNumber(oaiUntil) + UNTIL_IS_INCLUSIVE


    def _getDocument(self, identifier):
        docId = self._getDocId(identifier)
        return self._getSearcher(identifier).doc(docId) if docId is not None else None

    def _getDocId(self, identifier):
        searcher = self._getSearcher(identifier)
        results = searcher.search(TermQuery(Term(IDENTIFIER_FIELD, identifier)), 1)
        if results.totalHits.value == 0:
            return None
        return results.scoreDocs[0].doc

    def _updateOaiRecord(self, identifier, setSpecs, metadataPrefixes, delete=False, oldDoc=None, deleteInSets=None, deleteInPrefixes=None, _overrideStamp=None):
        oldDoc = oldDoc or self._getDocument(identifier)
        doc, oldDeletedSets, oldDeletedPrefixes = self._getNewDocument(identifier, oldDoc=oldDoc)
        newStamp = _overrideStamp if self._importMode else self._newStamp()
        doc.add(LongPoint(STAMP_FIELD, int(newStamp)))
        doc.add(StoredField(STAMP_FIELD, BytesRef(JArray('byte')(int_to_bytes(newStamp)))))
        doc.add(NumericDocValuesField(NUMERIC_STAMP_FIELD, int(newStamp)))

        allMetadataPrefixes, allDeletedPrefixes = self._setMetadataPrefixes(doc=doc, metadataPrefixes=asSet(metadataPrefixes), delete=delete, deleteInPrefixes=asSet(deleteInPrefixes), oldDeletedPrefixes=oldDeletedPrefixes)

        allSets, allDeletedSets = self._setSets(doc=doc, setSpecs=setSpecs or [], delete=delete, deleteInSets=deleteInSets, oldDeletedSets=oldDeletedSets)
        if delete or (allDeletedSets and allSets == allDeletedSets) or allMetadataPrefixes == allDeletedPrefixes:
            doc.add(StringField(TOMBSTONE_FIELD, TOMBSTONE_VALUE, Field.Store.YES))

        self._writer.updateDocument(Term(IDENTIFIER_FIELD, identifier), doc)
        self._latestModifications.add(str(identifier))
        self.do.signalOaiUpdate(metadataPrefixes=allMetadataPrefixes, sets=allSets, stamp=newStamp)

    def _getNewDocument(self, identifier, oldDoc, purgeSets=None):
        doc = Document()
        doc.add(StringField(IDENTIFIER_FIELD, identifier, Field.Store.YES))
        doc.add(IntPoint(HASH_FIELD, Partition.hashId(identifier)))
        oldDeletedSets = set()
        oldDeletedPrefixes = set()
        if oldDoc is not None:
            filterPurgedSets = lambda x: x
            if purgeSets:
                filterPurgedSets = lambda sets: [s for s in sets if s not in purgeSets]
            for oldPrefix in oldDoc.getValues(PREFIX_FIELD):
                doc.add(StringField(PREFIX_FIELD, oldPrefix, Field.Store.YES))
            for oldSet in filterPurgedSets(oldDoc.getValues(SETS_FIELD)):
                doc.add(StringField(SETS_FIELD, oldSet, Field.Store.YES))
            oldDeletedSets.update(oldDoc.getValues(SETS_DELETED_FIELD))
            oldDeletedPrefixes.update(filterPurgedSets(oldDoc.getValues(PREFIX_DELETED_FIELD)))
        return doc, oldDeletedSets, oldDeletedPrefixes

    def _newStamp(self):
        """time in microseconds"""
        newStamp = timestamp()
        if newStamp <= self._newestStamp:
            newStamp = self._newestStamp + 1
        self._newestStamp = newStamp
        return newStamp

    def _setMetadataPrefixes(self, doc, metadataPrefixes, delete, oldDeletedPrefixes, deleteInPrefixes):
        allMetadataPrefixes = set(doc.getValues(PREFIX_FIELD))
        allDeletedPrefixes = set(oldDeletedPrefixes)
        for prefix in metadataPrefixes:
            allDeletedPrefixes.discard(prefix)
        for prefix in metadataPrefixes.union(deleteInPrefixes):
            if prefix not in allMetadataPrefixes:
                doc.add(StringField(PREFIX_FIELD, prefix, Field.Store.YES))
                self._prefixes.setdefault(prefix, ('', ''))
                allMetadataPrefixes.add(prefix)
        allDeletedPrefixes.update(deleteInPrefixes)
        if delete:
            allDeletedPrefixes = allMetadataPrefixes

        for prefix in allDeletedPrefixes:
            doc.add(StringField(PREFIX_DELETED_FIELD, prefix, Field.Store.YES))

        return allMetadataPrefixes, allDeletedPrefixes

    def _setSets(self, doc, setSpecs, delete, deleteInSets, oldDeletedSets):
        currentSets = set(doc.getValues(SETS_FIELD))
        allSets = set(currentSets)
        for setSpec in _validSetSpecs(setSpecs):
            allSets.update(_setSpecAndSubsets(setSpec))
        if delete:
            allDeletedSets = set(allSets)
        else:
            allDeletedSets = set(oldDeletedSets)
            for setSpec in _validSetSpecs(setSpecs):
                allDeletedSets.difference_update(_setSpecAndSubsets(setSpec))
            if self._deleteInSetsSupport and deleteInSets:
                allSets.update(deleteInSets)
                allDeletedSets.update(deleteInSets)
        for aSet in allSets:
            if not aSet in currentSets:
                self._sets.setdefault(aSet, '')
                doc.add(StringField(SETS_FIELD, aSet, Field.Store.YES))
                allSets.add(aSet)
        for aSet in allDeletedSets:
            doc.add(StringField(SETS_DELETED_FIELD, aSet, Field.Store.YES))
        return allSets, allDeletedSets

    def _purge(self, identifier):
        self._writer.deleteDocuments(Term(IDENTIFIER_FIELD, identifier))

    def _purgeFromSet(self, setSpec):
        self._writer.deleteDocuments(Term(SETS_FIELD, setSpec))

    def _getStamp(self, identifier):
        doc = self._getDocument(identifier)
        if doc is None:
            return None
        return _stampFromDocument(doc)

    def _save(self):
        filename = join(self._directory, "data.json")
        with open(filename + "~", 'w') as f:
            dump(self._data, f)
        rename(filename + "~", filename)

    def _load(self):
        path = join(self._directory, "data.json")
        if isfile(path):
            with open(path) as fp:
                self._data = load(fp)
        else:
            self._data = dict(prefixes={}, sets={})

# helper methods

def getReader(path):
    return DirectoryReader.open(FSDirectory.open(Paths.get(path)))

def getLucene(path):
    directory = FSDirectory.open(Paths.get(path))
    analyzer = WhitespaceAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setIndexSort(Sort(SortField(NUMERIC_STAMP_FIELD, SortField.Type.LONG)))
    writer = IndexWriter(directory, config)
    reader = writer.getReader()
    searcher = IndexSearcher(reader)
    return writer, reader, searcher


class Record(object):
    def __init__(self, doc, requestedSets=None, requestedPrefix=None):
        self._doc = doc
        self._requestedSets = None if requestedSets is None else set(requestedSets)
        self._requestedPrefix = requestedPrefix

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
        if not hasattr(self, '_isDeleted'):
            self._isDeleted = self._doc.getField(TOMBSTONE_FIELD) is not None
        if self._isDeleted:
            return True
        if self._requestedPrefix in self.deletedPrefixes:
            return True
        if self._requestedSets is not None:
            matchingSets = self._requestedSets.intersection(self.sets)
            if matchingSets:
                return not bool(matchingSets.difference(self.deletedSets))
        return False

    @property
    def prefixes(self):
        if not hasattr(self, '_prefixes'):
            self._prefixes = set(self._doc.getValues(PREFIX_FIELD))
        return self._prefixes

    @property
    def deletedPrefixes(self):
        if not hasattr(self, '_deletedPrefixes'):
            self._deletedPrefixes = set(self._doc.getValues(PREFIX_DELETED_FIELD))
        return self._deletedPrefixes

    @property
    def sets(self):
        if not hasattr(self, '_sets'):
            self._sets = set(self._doc.getValues(SETS_FIELD))
        return self._sets

    @property
    def deletedSets(self):
        if not hasattr(self, '_deletedSets'):
            self._deletedSets = set(self._doc.getValues(SETS_DELETED_FIELD))
        return self._deletedSets

    def getDatestamp(self, preciseDatestamp=False):
        return stamp2zulutime(stamp=self.stamp, preciseDatestamp=preciseDatestamp)

    def asExportDict(self):
        self.isDeleted #side effect, sets _isDeleted
        return dict(
                identifier=self.identifier,
                timestamp=self.stamp,
                prefixes=sorted(self.prefixes),
                deletedPrefixes=sorted(self.deletedPrefixes),
                sets=sorted(self.sets),
                deletedSets=sorted(self.deletedSets),
                tombstone=self._isDeleted,
            )


def _setSpecAndSubsets(setSpec):
    subsets = setSpec.split(SETSPEC_HIERARCHY_SEPARATOR)
    for i in range(len(subsets), 0, -1):
        yield SETSPEC_HIERARCHY_SEPARATOR.join(subsets[0:i])

def allHierarchicalSetSpecs(setSpecs):
    result = set()
    for setSpec in setSpecs:
        result.update(_setSpecAndSubsets(setSpec))
    return result

def _validSetSpecs(setSpecs):
    for setSpec in setSpecs:
        if SETSPEC_SEPARATOR in setSpec:
            raise ValueError('SetSpec "%s" contains illegal characters' % setSpec)
        yield setSpec


def _stampFromDocument(doc):
    return bytes_to_int(doc.getField(STAMP_FIELD).binaryValue().bytes.bytes_)


def asSet(iterableOrNone):
    return set() if iterableOrNone is None else set(iterableOrNone)

def int_to_bytes(number):
    return number.to_bytes(length=(8 + (number + (number < 0)).bit_length()) // 8, byteorder='big')

def bytes_to_int(bin_data):
    return int.from_bytes(bin_data, byteorder='big')


SETSPEC_SEPARATOR = ","
SETSPEC_HIERARCHY_SEPARATOR = ":"

_MAX_MODIFICATIONS = 10000

PREFIX_FIELD = "prefix"
PREFIX_DELETED_FIELD = "prefixdeleted"
SETS_FIELD = "sets"
SETS_DELETED_FIELD = "setsdeleted"
IDENTIFIER_FIELD = "identifier"
STAMP_FIELD = "stamp"
HASH_FIELD = 'hash'
NUMERIC_STAMP_FIELD = "numeric_stamp"
TOMBSTONE_FIELD = "tombstone"
TOMBSTONE_VALUE = "T"

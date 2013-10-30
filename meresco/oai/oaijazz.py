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
# Copyright (C) 2011-2013 Seecr (Seek You Too B.V.) http://seecr.nl
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

import sys
from sys import maxint
from os.path import isdir, join, isfile
from os import makedirs, listdir, rename
from bisect import bisect_left
from time import time, strftime, gmtime, strptime
from calendar import timegm
from bsddb import btopen
from traceback import print_exc
from random import choice

from escaping import escapeFilename, unescapeFilename
from meresco.components.sorteditertools import OrIterator, AndIterator
from meresco.components import PersistentSortedIntegerList
from meresco.lucene import TermNumerator
from meresco.core import asyncreturn
from weightless.io import Suspend

from lucene import initVM
initVM()
from org.apache.lucene.document import Document, StringField, Field, LongField, FieldType
from org.apache.lucene.search import IndexSearcher, TermQuery, BooleanQuery, NumericRangeQuery, BooleanClause, TotalHitCountCollector, Sort, SortField
from org.apache.lucene.index import DirectoryReader, Term
from java.lang import Integer, Long

def getWriter(path):
    from java.io import File
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer
    from org.apache.lucene.index import IndexWriter, IndexWriterConfig
    from org.apache.lucene.util import Version
    directory = FSDirectory.open(File(path))
    analyzer = WhitespaceAnalyzer(Version.LUCENE_43)
    config = IndexWriterConfig(Version.LUCENE_43, analyzer)
    return IndexWriter(directory, config)

class OaiJazz(object):

    version = '5'

    def __init__(self, aDirectory, termNumerator=None, alwaysDeleteInPrefixes=None, preciseDatestamp=False, persistentDelete=True, maximumSuspendedConnections=100, autoCommit=True, name=None):
        self._directory = _ensureDir(aDirectory)
        self._versionFormatCheck()
        #self._deletePrefixes = alwaysDeleteInPrefixes or []
        self._preciseDatestamp = preciseDatestamp
        self._persistentDelete = persistentDelete
        self._maximumSuspendedConnections = maximumSuspendedConnections
        #self._autoCommit = autoCommit
        self._name = name
        self._suspended = {}
        self._prefixesInfoDir = _ensureDir(join(aDirectory, 'prefixesInfo'))
        self._prefixes = {}
        # TODO: geen test voor opslag 
        #self._setsDir = _ensureDir(join(aDirectory, 'sets'))
        self._sets = {}
        # TODO wat is dit?
        #self._changeFile = join(self._directory, 'change.json')
        self._read()
        self._writer = getWriter(aDirectory)
        self._newestStamp = self._newestStampFromIndex()

    def close(self):
        self._writer.close()

    def observable_name(self):
        return self._name

    def addOaiRecord(self, identifier, sets=None, metadataFormats=None):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        assert [prefix for prefix, schema, namespace in metadataFormats], 'No metadataFormat specified for record with identifier "%s"' % identifier
        doc = self._getDocument(identifier)
        if not doc:
            doc = Document()
        if metadataFormats:
            #doc.removeFields("prefix") # funny, prefixes are updates, sets not
            for prefix, schema, namespace in metadataFormats:
                self._metadataFormats[prefix] = (prefix, schema, namespace)
                doc.add(StringField("prefix", prefix, Field.Store.YES))
        if sets:
            doc.removeFields("sets")  # see funny above
            for setSpec, setName in sets:
                assert SETSPEC_SEPARATOR not in setSpec, 'SetSpec "%s" contains illegal characters' % setSpec
                doc.add(StringField("sets", setSpec, Field.Store.YES))
                self._sets[setSpec] = "?"
                if ":" in setSpec:
                    for set_ in setSpec.split(":"):
                        self._sets[set_] = "?"
                        doc.add(StringField("sets", set_, Field.Store.YES))
        doc.add(StringField("identifier", identifier, Field.Store.YES))
        doc.removeFields("thumbstone")
        newStamp = self._newStamp()
        doc.removeFields("stamp")
        doc.add(LongField("stamp", long(newStamp), Field.Store.YES))
        self._writer.updateDocument(Term("identifier", identifier), doc)
        self._resume()

    @asyncreturn
    def delete(self, identifier):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        doc = self._getDocument(identifier)
        if not doc:
            return
        doc.add(StringField("thumbstone", "True", Field.Store.YES))
        oldStamp = doc.getField("stamp").numericValue().longValue()
        newStamp = self._newStamp()
        doc.removeFields("stamp")
        doc.add(LongField("stamp", long(newStamp), Field.Store.YES))
        self._writer.updateDocument(Term("identifier", identifier), doc)
        self._resume()

    def _purge(self, identifier):
        self._writer.deleteDocuments(Term("identifier", identifier))

    def purge(self, identifier):
        if self._persistentDelete:
            raise KeyError("Purging of records is not allowed with persistent deletes.")
        return self._purge(identifier)

    def _getSearcher(self):
        oldReader = self._writer.getReader()
        newReader = DirectoryReader.openIfChanged(oldReader, self._writer, True) or oldReader
        return IndexSearcher(newReader)

    def oaiSelect(self, sets=None, prefix='oai_dc', continueAfter='0', oaiFrom=None, oaiUntil=None, setsMask=None):
        start = max(int(continueAfter)+1, self._fromTime(oaiFrom))
        stop = self._untilTime(oaiUntil) or Long.MAX_VALUE
        searcher = self._getSearcher()
        fromRange = NumericRangeQuery.newLongRange("stamp", 8, start, stop, True, True)
        query = BooleanQuery()
        query.add(fromRange, BooleanClause.Occur.MUST)
        query.add(TermQuery(Term("prefix", prefix)), BooleanClause.Occur.MUST)
        if sets:
            sq = BooleanQuery()
            for setSpec in sets:
                sq.add(TermQuery(Term("sets", setSpec)), BooleanClause.Occur.SHOULD)
            if ":" in setSpec:
                for set_ in setSpec.split(":"):
                    sq.add(TermQuery(Term("sets", set_)), BooleanClause.Occur.MUST)
            query.add(sq, BooleanClause.Occur.MUST)
        if setsMask:
            for set_ in setsMask:
                query.add(TermQuery(Term("sets", set_)), BooleanClause.Occur.MUST)
        results = searcher.search(query, 200, Sort(SortField("stamp", SortField.Type.LONG)))
        for result in results.scoreDocs:
            yield searcher.doc(result.doc).getField("identifier").stringValue()
        return

        #TODO: RecordId gebruiken?
        #return (RecordId(self._termNumerator.getTerm(int(identifierID)), stampId)
        #        for identifierID, stampId in idAndStamps if not identifierID is None)

    def _getDocument(self, identifier):
        searcher = self._getSearcher()
        results = searcher.search(TermQuery(Term("identifier", identifier)), 1)
        if results.totalHits == 0:
            return None
        return searcher.doc(results.scoreDocs[0].doc)

    def _getStamp(self, identifier):
        doc = self._getDocument(identifier)
        if not doc:
            return None
        return doc.getField("stamp").numericValue().longValue()

    def getDatestamp(self, identifier):
        stamp = self._getStamp(identifier)
        if stamp is None:
            return None
        return _stamp2zulutime(stamp=stamp, preciseDatestamp=self._preciseDatestamp)

    def getUnique(self, identifier):
        if hasattr(identifier, 'stamp'):
            return identifier.stamp
        return self._getStamp(identifier)

    def isDeleted(self, identifier):
        doc = self._getDocument(identifier)
        return doc.get("thumbstone") == "True"

    def getAllMetadataFormats(self):
        for prefix, schema, namespace in self._metadataFormats.values():
            yield (prefix, schema, namespace)

    def getAllPrefixes(self):
        return self._metadataFormats.keys()

    def getSets(self, identifier):
        doc = self._getDocument(identifier)
        if not doc:
            return []
        return doc.getValues("sets")

    def getPrefixes(self, identifier):
        doc = self._getDocument(identifier)
        if not doc:
            return []
        return doc.getValues("prefix")

    def getAllSets(self):
        return self._sets.keys()

    def getNrOfRecords(self, prefix='oai_dc'):
        searcher = self._getSearcher()
        collector = TotalHitCountCollector()
        searcher.search(TermQuery(Term("prefix", prefix)), collector)
        return collector.getTotalHits()

    #TODO: onhandig in Lucene
    def getLastStampId(self, prefix='oai_dc'):
        if prefix in self._prefixes and self._prefixes[prefix]:
            stampIds = self._prefixes[prefix]
            return stampIds[-1] if stampIds else None

    def getDeletedRecordType(self):
        return "persistent" if self._persistentDelete else "transient"

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

    def commit(self):
        self._writer.commit()

    def handleShutdown(self):
        print 'handle shutdown: saving OaiJazz %s' % self._directory
        from sys import stdout; stdout.flush()
        self.commit()

    # private methods

    def _read(self):
        self._metadataFormats = {}
        #for prefix in (unescapeFilename(name[:-len('.list')]) for name in listdir(self._prefixesDir) if name.endswith('.list')):
        for prefix in (unescapeFilename(name[:-len('.schema')]) for name in listdir(self._prefixesInfoDir) if name.endswith('.schema')):
            #self._getPrefixList(prefix)
            schema = open(join(self._prefixesInfoDir, '%s.schema' % escapeFilename(prefix))).read()
            namespace = open(join(self._prefixesInfoDir, '%s.namespace' % escapeFilename(prefix))).read()
            self._metadataFormats[prefix] = (prefix, schema, namespace)

        return

        for setSpec in (unescapeFilename(name[:-len('.list')]) for name in listdir(self._setsDir) if name.endswith('.list')):
            self._getSetList(setSpec)

    def _getSetList(self, setSpec):
        if setSpec not in self._sets:
            filename = join(self._setsDir, '%s.list' % escapeFilename(setSpec))
            l = self._sets[setSpec] = PersistentSortedIntegerList(filename, autoCommit=self._autoCommit)
            self._removeInvalidStamp(l)
            self._newestStampFromList(l)
        return self._sets[setSpec]

    def _getPrefixList(self, prefix):
        if prefix not in self._prefixes:
            filename = join(self._prefixesDir, '%s.list' % escapeFilename(prefix))
            l = self._prefixes[prefix] = PersistentSortedIntegerList(filename, autoCommit=self._autoCommit)
            self._removeInvalidStamp(l)
            self._newestStampFromList(l)
        return self._prefixes[prefix]

    def _newestStampFromIndex(self):
        searcher = self._getSearcher()
        maxDoc = searcher.getIndexReader().maxDoc()
        if maxDoc > 0:
            s = searcher.doc(maxDoc - 1).getField("stamp")
        else:
            return 0

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

    def _storeMetadataFormats(self, metadataFormats):
        for prefix, schema, namespace in metadataFormats:
            if (prefix, schema, namespace) != self._metadataFormats.get(prefix):
                self._metadataFormats[prefix] = (prefix, schema, namespace)
                _write(join(self._prefixesInfoDir, '%s.schema' % escapeFilename(prefix)), schema)
                _write(join(self._prefixesInfoDir, '%s.namespace' % escapeFilename(prefix)), namespace)

    def _newStamp(self):
        """time in microseconds"""
        newStamp = int(time() * DATESTAMP_FACTOR)
        if newStamp <= self._newestStamp:
            newStamp = self._newestStamp + 1
        self._newestStamp = newStamp
        return newStamp

    def _versionFormatCheck(self):
        self._versionFile = join(self._directory, "oai.version")
        assert listdir(self._directory) == [] or (isfile(self._versionFile) and open(self._versionFile).read() == self.version), "The OAI index at %s need to be converted to the current version (with 'convert_oai_v3_to_v4' in meresco-oai/bin)" % self._directory
        with open(join(self._directory, "oai.version"), 'w') as f:
            f.write(self.version)

    def _resume(self):
        while len(self._suspended) > 0:
            clientId, suspend = self._suspended.popitem()
            suspend.resume()

    def _removeInvalidStamp(self, l):
        # Last or second last could be unused stamps due to crashes
        if l:
            for stamp in l[-2:]:
                if self._getIdentifier(str(stamp)) is None:
                    l.remove(stamp)

    def _removeIfInList(self, item, l):
        try:
            l.remove(item)
        except ValueError:
            pass

    def _appendIfNotYet(self, item, l):
        if len(l) == 0 or l[-1] != item:
            l.append(item)


# helper methods

class RecordId(str):
    def __new__(self, identifierID, stamp):
        return str.__new__(self, identifierID)
    def __init__(self, identifierID, stamp):
        self.stamp = stamp
    def __getslice__(self, *args, **kwargs):
        return RecordId(str.__getslice__(self, *args, **kwargs), self.stamp)

def _writeLines(filename, lines):
    with open(filename + '.tmp', 'w') as f:
        for line in lines:
            f.write('%s\n' % line)
    rename(filename + '.tmp', filename)

def _write(filename, content):
    with open(filename + '.tmp', 'w') as f:
        f.write(content)
    rename(filename + '.tmp', filename)

def _flattenSetHierarchy(sets):
    """"[1:2:3, 1:2:4] => [1, 1:2, 1:2:3, 1:2:4]"""
    result = set()
    for setSpec in sets:
        parts = setSpec.split(':')
        for i in range(1, len(parts) + 1):
            result.add(':'.join(parts[:i]))
    return result

def safeString(aString):
    return str(aString) if isinstance(aString, unicode) else aString

def stamp2zulutime(stamp):
    if stamp is None:
        return ''
    return _stamp2zulutime(int(stamp))

def _stamp2zulutime(stamp, preciseDatestamp=False):
    microseconds = ".%s" % (stamp % DATESTAMP_FACTOR) if preciseDatestamp else ""
    return "%s%sZ" % (strftime('%Y-%m-%dT%H:%M:%S', gmtime(stamp / DATESTAMP_FACTOR)), microseconds)

def _ensureDir(directory):
    isdir(directory) or makedirs(directory)
    return directory


class ForcedResumeException(Exception):
    pass

SETSPEC_SEPARATOR = ','
DATESTAMP_FACTOR = 1000000

IDENTIFIER2SETSPEC = 'ss:'
STAMP2IDENTIFIER = 'st:'

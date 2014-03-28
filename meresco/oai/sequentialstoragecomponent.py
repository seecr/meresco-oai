## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.oai import SequentialMultiStorage
from cStringIO import StringIO
from time import time
from os.path import join

imported = False
def lazyImport():
    global imported
    if imported:
        return
    imported = True

    from oaijazz import importVM
    importVM()

    from java.io import File
    from org.apache.lucene.document import Document, StringField, Field, LongField, FieldType
    from org.apache.lucene.search import IndexSearcher, TermQuery
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.util import Version
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer

    StampType = FieldType()
    StampType.setIndexed(False)
    StampType.setStored(True)
    StampType.setNumericType(FieldType.NumericType.LONG)

    globals().update(locals())


class SequentialStorageComponent(object):
    def __init__(self, path):
        self._directory = join(path, "data")
        self._storage = SequentialMultiStorage(self._directory)
        self._index = _Index(path + "/index")
        self._last_stamp = 0

    def isEmpty(self):
        return self._storage.isEmpty()

    def add(self, identifier, partname, data):
        stamp = int(time() * 1000000)
        while stamp <= self._last_stamp:
            stamp += 1
        self._last_stamp = stamp
        self._index[str(identifier)] = stamp
        self._storage.addData(stamp, partname, data)
        return
        yield

    def delete(self, identifier):
        del self._index[str(identifier)]
        return
        yield

    def isAvailable(self, identifier, partname):
        try:
            self._index[str(identifier)]
            return True, True
        except KeyError:
            return False, False

    def _getData(self, identifier, partname):
        stamp = self._index[str(identifier)]
        return self._storage.getData(stamp, partname)

    def getStream(self, identifier, partname):
        return StringIO(self._getData(identifier, partname))

    def yieldRecord(self, identifier, partname):
        yield self._getData(identifier, partname)

    def handleShutdown(self):
        print 'handle shutdown: saving SequentialStorageComponent %s' % self._directory
        from sys import stdout; stdout.flush()
        self._storage.handleShutdown()
        self._index.close()


class _Index(object):
    def __init__(self, path):
        lazyImport()
        self._writer, self._reader, self._searcher = _getLucene(path)
        self._latestModifications = {}
        self._doc = Document()
        self._keyField = StringField("key", "", Field.Store.NO)
        self._valueField = LongField("value", 0L, StampType)
        self._doc.add(self._keyField)
        self._doc.add(self._valueField)

    def __setitem__(self, key, value):
        self._maybeReopen()
        self._keyField.setStringValue(key)
        self._valueField.setLongValue(long(value))
        self._writer.updateDocument(Term("key", key), self._doc)
        self._latestModifications[key] = value

    def __getitem__(self, key):
        stamp = self._latestModifications.get(key)
        if stamp == DELETED_RECORD:
            raise KeyError("Record deleted")
        elif stamp is not None:
            return stamp
        self._maybeReopen()
        topDocs = self._searcher.search(TermQuery(Term("key", key)), 1)
        if topDocs.totalHits == 0:
            raise KeyError("Record deleted")
        return self._searcher.doc(topDocs.scoreDocs[0].doc).getField("value").numericValue().longValue()

    def __delitem__(self, key):
        self._writer.deleteDocuments(Term("key", key))
        self._latestModifications[key] = DELETED_RECORD

    def _maybeReopen(self):
        if len(self._latestModifications) > 10000:
            self._reader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
            self._searcher = IndexSearcher(self._reader)
            self._latestModifications.clear()

    def close(self):
        self._writer.close()


def _getLucene(path):
    directory = FSDirectory.open(File(path))
    config = IndexWriterConfig(Version.LUCENE_43, None)
    config.setRAMBufferSizeMB(256.0) # faster
    #confif.setUseCompoundFile(false) # faster, for Lucene 4.4 and later
    writer = IndexWriter(directory, config)
    reader = writer.getReader()
    searcher = IndexSearcher(reader)
    return writer, reader, searcher

DELETED_RECORD = object()

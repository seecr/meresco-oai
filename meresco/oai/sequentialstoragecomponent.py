from meresco.oai import SequentialMultiStorage
from meresco.core import asyncnoreturnvalue
from cStringIO import StringIO
from time import time
from oaijazz import lazyImport
from time import sleep


from lucene import initVM, getVMEnv
initVM()

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

def getLucene(path):
    directory = FSDirectory.open(File(path))
    analyzer = WhitespaceAnalyzer(Version.LUCENE_43)
    config = IndexWriterConfig(Version.LUCENE_43, analyzer)
    writer = IndexWriter(directory, config)
    reader = writer.getReader()
    searcher = IndexSearcher(reader)
    return writer, reader, searcher

class Index(object):
    def __init__(self, path):
        self._writer, self._reader, self._searcher = getLucene(path)
        self._latestModifications = {}

    def __setitem__(self, key, value):
        doc = Document()
        doc.add(StringField("key", key, Field.Store.NO))
        doc.add(LongField("value", long(value), Field.Store.YES))
        self._writer.updateDocument(Term("key", key), doc)
        self._latestModifications[key] = value

    def __getitem__(self, key):
        stamp = self._latestModifications.get(key)
        if stamp:
            return stamp
        if len(self._latestModifications) > 10000:
            newreader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
            if newreader:
                self._reader = newreader
                self._searcher = IndexSearcher(newreader)
                self._latestModifications.clear()
        doc = self._searcher.search(TermQuery(Term("key", key)), 1).scoreDocs[0].doc
        return self._searcher.doc(doc).getField("value").numericValue().longValue()

class SequentialStorageComponent(object):
    def __init__(self, path):
        self._storage = SequentialMultiStorage(path + "/data")
        self._index = Index(path + "/index")

    def isEmpty(self):
        return self._storage.isEmpty()

    @asyncnoreturnvalue
    def add(self, identifier, partname, data):
        stamp = int(time() * 1000000)
        sleep(0.000001)
        self._index[identifier] = stamp
        self._storage.addData(stamp, partname, data)

    def isAvailable(self, identifier, partname):
        return False, False

    def getStream(self, identifier, partname):
        stamp = self._index[identifier]
        return StringIO(self._storage.getData(stamp, partname))

## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from os import system
from os.path import dirname, join, abspath, isdir
from shutil import copytree

from seecr.test import SeecrTestCase
from org.apache.lucene.index import IndexWriterConfig, IndexWriter, MultiDocValues
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.util import Version
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.store import FSDirectory
from java.io import File
from simplejson import load

mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertOaiV5ToV6Test(SeecrTestCase):
    def testConversion(self):
        datadir = join(self.tempdir, 'oai_conversion_v5_to_v6')
        copytree(join(mypath, 'data', 'oai_conversion_v5_to_v6'), datadir)
        system("%s %s > %s 2>&1" % (
                join(binDir, 'convert_oai_v5_to_v6'),
                datadir,
                join(self.tempdir, 'oai_conversion_v5_to_v6.log'),
            ))
        # print open(join(self.tempdir, 'oai_conversion_v5_to_v6.log')).read()

        directory = FSDirectory.open(File(datadir))
        analyzer = WhitespaceAnalyzer(Version.LUCENE_43)
        config = IndexWriterConfig(Version.LUCENE_43, analyzer)
        writer = IndexWriter(directory, config)
        reader = writer.getReader()
        searcher = IndexSearcher(reader)

        self.assertEquals(130, reader.maxDoc()) # Old index maxdoc is 134, but it contains 4 deletes
        self.assertEquals(130, reader.numDocs())

        stamps = MultiDocValues.getNumericValues(reader, "stamp")
        self.assertNotEquals(None, stamps)
        stampValues = []
        prefixes = set()
        sets = set()
        for i in range(reader.maxDoc()):
            stampValues.append(stamps.get(i))
            doc = searcher.doc(i)
            fields = [f.name() for f in doc.getFields()]
            prefixes.update(set(doc.getValues('prefix')))
            sets.update(set(doc.getValues('sets')))
            if doc.get('identifier') == 'http://zp.seecr.nl/rdf/to_be_deleted':
                self.assertEquals(['identifier', 'stamp', 'prefix', 'sets', 'tombstone'], fields)
            else:
                self.assertEquals(['identifier', 'stamp', 'prefix', 'sets'], fields)

        self.assertEquals(set(['rdf']), prefixes)
        self.assertEquals(set(['consumentenbond', 'krantenbank', 'dsdp-ihlia', 'seecr', 'nbd-biblion', 'gids', 'winklerprins', 'seecr2', 'dsdp-uitburo', 'gutenberg', 'webggc.oclc.nl', 'cdr']), sets)

        self.assertEquals(130, len(stampValues))
        self.assertEquals(stampValues, sorted(set(stampValues)))

        self.assertEquals({"prefixes": {"rdf": ["", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"]}, "sets": {"consumentenbond": "consumentenbond", "krantenbank": "krantenbank", "dsdp-ihlia": "dsdp-ihlia", "gids": "gids", "nbd-biblion": "nbd-biblion", "seecr": "seecr", "winklerprins": "winklerprins", "gutenberg": "gutenberg", "dsdp-uitburo": "dsdp-uitburo", "seecr2": "seecr2", "webggc.oclc.nl": "webggc.oclc.nl", "cdr": "cdr"}}, load(open(join(datadir, 'data.json'))))

        self.assertEquals('6', open(join(datadir, 'oai.version')).read())

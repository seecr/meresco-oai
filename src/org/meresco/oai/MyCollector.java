/* begin license *
 *
 * "Meresco Oai" are components to build Oai repositories, based on
 * "Meresco Core" and "Meresco Components".
 *
 * Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 *
 * This file is part of "Meresco Oai"
 *
 * "Meresco Oai" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Oai" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Oai"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.oai;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.search.IndexSearcher;
import java.util.Set;
import java.util.HashSet;

public class MyCollector extends Collector {

    private int[] hits;
    private int hitCount = 0;
    private int docBase;
    private boolean shouldCountHits;
    public boolean moreRecordsAvailable = false;


    public MyCollector(int maxDocsToCollect, boolean shouldCountHits) {
        this.hits = new int[maxDocsToCollect];
        this.shouldCountHits = shouldCountHits;
    }

    public int[] hits() {
        if (this.hitCount < this.hits.length) {
            int[] hits = new int[this.hitCount];
            System.arraycopy(this.hits, 0, hits, 0, this.hitCount);
            this.hits = hits;
        }
        return this.hits;
    }

    public Document[] docs(IndexSearcher searcher) throws IOException {
        Set<String> fieldsToVisit = new HashSet<String>(4);
        fieldsToVisit.add("identifier");
        fieldsToVisit.add("stamp");
        fieldsToVisit.add("sets");
        fieldsToVisit.add("tombstone");
        int[] hits = this.hits();
        Document[] docs = new Document[hits.length];
        for (int i=0; i<hits.length; i++) {
            docs[i] = searcher.doc(hits[i], fieldsToVisit);
        }
        return docs;
    }

    public int totalHits() {
        return this.hitCount;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.docBase = context.docBase;
    }

    @Override
    public void collect(int doc) throws IOException {
        this.hitCount++;
        if (this.hitCount > this.hits.length) {
            this.moreRecordsAvailable = true;
            if (!this.shouldCountHits)
                throw new CollectionTerminatedException();
        } else {
            this.hits[this.hitCount - 1] = this.docBase + doc;
        }
    }

    @Override
    public void setScorer(Scorer scorer) {

    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }
}
/* begin license *
 *
 * "Meresco Oai" are components to build Oai repositories, based on
 * "Meresco Core" and "Meresco Components".
 *
 * Copyright (C) 2013-2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.sorter.EarlyTerminatingSortingCollector;
import java.util.Set;
import java.util.HashSet;


public class OaiSortingCollector extends Collector {
    private static final String NUMERIC_STAMP_FIELD = "numeric_stamp";
    private int hitCount = 0;
    private boolean shouldCountHits;
    private boolean delegateTerminated = false;
    private EarlyTerminatingSortingCollector earlyCollector;
    private TopFieldCollector topDocsCollector;
    public boolean moreRecordsAvailable = false;
    public int maxDocsToCollect;
    private NumericDocValues stamps;
    private long start;
    private long stop;


    public OaiSortingCollector(int maxDocsToCollect, boolean shouldCountHits, long start, long stop) throws IOException {
        this.topDocsCollector = TopFieldCollector.create(new Sort(new SortField(NUMERIC_STAMP_FIELD, SortField.Type.LONG)), maxDocsToCollect, false, false, false, false);
        this.earlyCollector = new EarlyTerminatingSortingCollector(this.topDocsCollector, new Sort(new SortField(NUMERIC_STAMP_FIELD, SortField.Type.LONG)), maxDocsToCollect + 1);
        this.maxDocsToCollect = maxDocsToCollect;
        this.shouldCountHits = shouldCountHits;
        this.start = start;
        this.stop = stop;
    }

    public Document[] docs(IndexSearcher searcher) throws IOException {
        Set<String> fieldsToVisit = new HashSet<String>(4);
        fieldsToVisit.add("identifier");
        fieldsToVisit.add("stamp");
        fieldsToVisit.add("sets");
        fieldsToVisit.add("tombstone");
        ScoreDoc[] hits = this.topDocsCollector.topDocs().scoreDocs;
        Document[] docs = new Document[hits.length];
        for (int i=0; i<hits.length; i++) {
            docs[i] = searcher.doc(hits[i].doc, fieldsToVisit);
        }
        return docs;
    }

    public int remainingRecords() {
        if (this.shouldCountHits) {
            return Math.max(0, this.hitCount - this.maxDocsToCollect);
        }
        return -1;
    }

    public int totalHits() {
        return this.hitCount;
    }

    @Override
    public void collect(int doc) throws IOException {
        long stamp = this.stamps.get(doc);
        if (stamp < this.start || stamp > this.stop)
            return;
        this.hitCount++;
        if (this.hitCount > this.maxDocsToCollect) {
            this.moreRecordsAvailable = true;
        }
        if (delegateTerminated) {
            return;
        }
        try {
            this.earlyCollector.collect(doc);
        }
        catch (CollectionTerminatedException e) {
            delegateTerminated = true;
            if (!this.shouldCountHits) {
                throw e;
            }
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.earlyCollector.setScorer(scorer);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.delegateTerminated = false;
        this.earlyCollector.setNextReader(context);
        this.stamps = context.reader().getNumericDocValues("numeric_stamp");
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return this.earlyCollector.acceptsDocsOutOfOrder();
    }

}

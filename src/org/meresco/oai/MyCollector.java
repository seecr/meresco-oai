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

public class MyCollector extends Collector {

    private int[] hits;
    private int hitCount = 0;
    private int docBase;
    private boolean shouldCountHits;

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

    public int totalHits() {
        return this.hitCount;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.docBase = context.docBase;
    }

    @Override
    public void collect(int doc) throws IOException {
        hitCount++;
        if (hitCount - 1 >= this.hits.length) {
            if (!shouldCountHits)
                throw new CollectionTerminatedException();
        } else {
            this.hits[hitCount - 1] = this.docBase + doc;
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
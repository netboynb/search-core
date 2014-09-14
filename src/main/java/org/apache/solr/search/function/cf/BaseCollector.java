package org.apache.solr.search.function.cf;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

/**
 * 
 * 
 * @author netboy 2013-7-15下午03:36:51
 */
public abstract class BaseCollector extends Collector {
	protected IndexReader reader;
	protected Scorer scorer;
	protected int docBase = 0;
	@Override
	public void setScorer(Scorer scorer) throws IOException {
		this.scorer = scorer;
	}

	@Override
	public abstract void collect(int doc) throws IOException;

	@Override
	public void setNextReader(AtomicReaderContext context) throws IOException {
		this.reader = context.reader();
		this.docBase = context.docBase;
	}

	@Override
	public boolean acceptsDocsOutOfOrder() {
		return false;
	}
}

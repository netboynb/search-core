package org.apache.solr.search.function.cf;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
/**
 *
 * @author netboy 2014年9月8日下午7:01:33
 */
public interface FilterableCollector {
	public boolean matchs(int doc);

	public void setNextReader(AtomicReaderContext context) throws IOException;

	public void setScorer(Scorer scorer) throws IOException;
}

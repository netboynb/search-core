package org.apache.solr.search.function.cf;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;


/**
 * 
 * 
 * @author netboy 2013-7-15下午03:46:10
 */
public class FilterCollector extends BaseCollector {
	protected Collector delegatedCollector;

	protected FilterableCollector[] filters;

	/** 多个 filter 的组合关系 */
	protected boolean andOperate = true;

	FilterCollector() {

	}

	/**
	 * filters.matchs(docId) 命中的才调用 delegatedCollector.collect(doc)
	 * 
	 * @param delegatedCollector
	 * @param filters
	 */
	public FilterCollector(Collector delegatedCollector, List<FilterableCollector> filters) {
		this(delegatedCollector, filters.toArray(new FilterableCollector[0]));
	}

	/**
	 * @param delegatedCollector
	 * @param filters
	 * @see #FilterCollector(Collector, List)
	 */
	public FilterCollector(Collector delegatedCollector, FilterableCollector[] filters) {
		super();
		this.delegatedCollector = delegatedCollector;
		this.filters = filters;
	}

	@Override
	public void collect(int doc) throws IOException {
		if(filters != null) {
			if(andOperate) { // and
				boolean isCollect = true;
				for(FilterableCollector filter : filters) {
					if(!filter.matchs(doc)) {
						isCollect = false;
						break;
					}
				}
				if(isCollect) {
					delegatedCollector.collect(doc);
				}
			} else { // or
				boolean isCollect = false;
				for(FilterableCollector filter : filters) {
					if(filter.matchs(doc)) {
						isCollect = true;
						break;
					}
				}
				if(isCollect) {
					delegatedCollector.collect(doc);
				}
			}

		} else {
			delegatedCollector.collect(doc);
		}
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {
		super.setScorer(scorer);
		if(filters != null) {
			for(FilterableCollector filter : filters) {
				filter.setScorer(scorer);
			}
		}
		delegatedCollector.setScorer(scorer);
	}

	@Override
	public void setNextReader(AtomicReaderContext context) throws IOException {
		super.setNextReader(context);
		if(filters != null) {
			for(FilterableCollector filter : filters) {
				filter.setNextReader(context);
			}
		}
		delegatedCollector.setNextReader(context);
	}

	@Override
	public boolean acceptsDocsOutOfOrder() {
		return delegatedCollector.acceptsDocsOutOfOrder();
	}

	public boolean isAndOperate() {
		return andOperate;
	}

	public void setAndOperate(boolean andOperate) {
		this.andOperate = andOperate;
	}

}
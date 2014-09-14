package org.apache.solr.search.function.cf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.search.Scorer;
/**
 * put the query's cf.in values into set
 * 
 * @author netboy 2014年9月8日下午7:01:33
 */
public class InFilterableCollector implements FilterableCollector {
	
	protected String type;
	
	protected final FieldCacheSource cacheSource;
	protected final Set<Object> values;
	protected String fieldType;
	
	private FunctionValues currentReaderDocValues;
	
	public InFilterableCollector(FieldSourceAndValues fsav) {
		this(fsav.getCacheSource(), fsav.getFieldTypeName(), fsav.getValues().toArray(new String[0]));
	}
	
	/**
	 * @param cacheSource 缓存字段值.
	 * @param fieldValues in 关系的 value，如 [1, 2, 3, 4]，表示这个字段 in (1,2,3,4)
	 * @param fieldTypeName 指定字段所属的字段类型
	 * @throws IllegalArgumentException fieldValues 为空，或 cacheSource.toObject 抛出
	 * NumberFormatException
	 */
	public InFilterableCollector(FieldCacheSource cacheSource, String fieldTypeName, String... fieldValues) {
		this.cacheSource = cacheSource;
		values = new HashSet<Object>();
		if(fieldValues == null || fieldValues.length < 1) {
			throw new IllegalArgumentException(cacheSource.description()+" in query, q value is blank");
		} else {
			for(int i=0; i<fieldValues.length; i++) {
				try {
					Object obj = ToolUtils.numStr2Obj(fieldTypeName, fieldValues[i]);
					values.add(obj);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(cacheSource.description()+" in query, ["+fieldValues[i]+"] can't parse value", e);
				}
			}
		}
		
		type = SearchParams.CF_IN;
	}
	
	protected InFilterableCollector(FieldCacheSource cacheSource, Set<Object> values) {
		this.cacheSource = cacheSource;
		this.values = values;
		
		type = SearchParams.CF_IN;
	}
	

	@Override
	public boolean matchs(int doc) {
		if(currentReaderDocValues == null) {
			return false;
		}
		Object obj = currentReaderDocValues.objectVal(doc);
		return values.contains(obj);
	}

	@Override
	public void setNextReader(AtomicReaderContext context) throws IOException {
		currentReaderDocValues = cacheSource.getValues(null, context);
	}

	protected FunctionValues getCurrentReaderDocValues() {
		return currentReaderDocValues;
	}

	public String toString() {
		return getClass().getSimpleName()+"-"+type+"="+cacheSource.getField()+" in "+values.toString();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cacheSource == null) ? 0 : cacheSource.hashCode());
		result = prime * result + ((currentReaderDocValues == null) ? 0 : currentReaderDocValues.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InFilterableCollector other = (InFilterableCollector) obj;
		if (cacheSource == null) {
			if (other.cacheSource != null)
				return false;
		} else if (!cacheSource.equals(other.cacheSource))
			return false;
		if (currentReaderDocValues == null) {
			if (other.currentReaderDocValues != null)
				return false;
		} else if (!currentReaderDocValues.equals(other.currentReaderDocValues))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {
	}

}

package org.apache.solr.search.function.cf;

import java.util.List;

import org.apache.lucene.queries.function.valuesource.FieldCacheSource;

public class FieldSourceAndValues {

	private FieldCacheSource cacheSource;
	private List<String> values;
	private String fieldTypeName;

	public FieldSourceAndValues(FieldCacheSource cacheSource, List<String> values, String fieldTypeName) {
		if(cacheSource == null) {
			throw new IllegalArgumentException("FieldCacheSource can't be null!");
		}
		this.cacheSource = cacheSource;
		this.values = values;
		this.fieldTypeName = fieldTypeName;

	}

	public String getField() {
		return cacheSource.getField();
	}
	
	public FieldCacheSource getCacheSource() {
		return cacheSource;
	}

	public void setCacheSource(FieldCacheSource cacheSource) {
		this.cacheSource = cacheSource;
	}

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}

	public String getFieldTypeName() {
		return fieldTypeName;
	}
	
}

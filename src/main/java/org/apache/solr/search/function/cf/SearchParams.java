package org.apache.solr.search.function.cf;

/**
 *
 * @author netboy 2014年9月8日下午5:44:22
 */
public interface SearchParams {
	/**
	 * set collect filter 'in' tag
	 */
	public static final String CF_IN = "cf.in";
	public static final int INT_TYPE = 1;
	public static final int LONG_TYPE = 2;
	public static final int FLOAT_TYPE = 3;
	public static final int DOUBLe_TYPE = 4;
}

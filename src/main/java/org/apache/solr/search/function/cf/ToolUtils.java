package org.apache.solr.search.function.cf;

/**
 *
 * @author netboy 2014年9月9日下午3:32:00
 */
public class ToolUtils {

	/**
	 * translate number string to number object
	 */
	public static Object numStr2Obj(String fieldType, String value) {
		Object object = null;
		if(fieldType.equals("int")) {
			object = Integer.parseInt(value);
		} else if(fieldType.equals("long")) {
			object = Long.parseLong(value);
		} else if(fieldType.equals("float")) {
			object = Float.parseFloat(value);
		} else if(fieldType.equals("double")) {
			object = Double.parseDouble(value);
		} else {
			throw new IllegalArgumentException("cache field's fieldType must be : int or long or float or double");
		}
		return object;
	}
}

package org.apache.dstream;

public interface Distributable {

	public static String DSTR_PREFIX = "dstream";
	
	public static String SRC_SUPPLIER = DSTR_PREFIX + ".supplier";
	
	public static String SRC_URL_SUPPLIER = DSTR_PREFIX + ".supplier.url";
	
	public static String PARTITIONER = DSTR_PREFIX + ".partitioner";
}

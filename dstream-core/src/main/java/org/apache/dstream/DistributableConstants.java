package org.apache.dstream;

public interface DistributableConstants {

	public static String DSTR_PREFIX = "dstream.";
	
	public static String SOURCE= DSTR_PREFIX + "source.";
	
	public static String OUTPUT = DSTR_PREFIX + "output";
	
	public static String PARTITIONER = DSTR_PREFIX + "partitioner";
	
	public static String STAGE = DSTR_PREFIX + "stage.";
	
	public static String PARALLELISM = STAGE + "parallelizm.";
	
	public static String MAP_SIDE_COMBINE = STAGE + "ms_combine.";
	
	public static String GENERATE_CONF = "generateConfig:";
	
}

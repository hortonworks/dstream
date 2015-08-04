package org.apache.dstream;

import java.net.URI;

/**
 * Constant values used by the framework 
 */
public interface DistributableConstants {

	public static String DSTR_PREFIX = "dstream.";
	
	// Configuration constants
	/**
	 * Identifies the source {@link URI} of the individual pipeline
	 * For example, <i>dstream.source.foo=hdfs://hadoop.com/demo/monte-cristo.txt</i><br>
	 * In the above 'foo' would be the name of the pipeline
	 */
	public static String SOURCE= DSTR_PREFIX + "source.";
	
	/**
	 * 
	 */
	public static String DELEGATE= DSTR_PREFIX + "delegate";
	
	/**
	 * Identifies the output directory {@link URI} of the execution identified by 
	 * name (e.g., <i>stream.executeAs("myExecution")</i>).
	 * For example, <i>dstream.output.myExecution=hdfs://hadoop.com/out</i><br>
	 */
	public static String OUTPUT = DSTR_PREFIX + "output";

	/**
	 * {@link #STAGE} related configuration.<br>
	 * Identifies stage parallelism (e.g., <i>dstream.stage.parallelizm.1_hash=3</i>)<br>
	 * In the above '1_hash' identifies combination of 'stage id' + "_" + 'pipeline name' 
	 */
	public static String PARALLELISM = DSTR_PREFIX + "parallelizm";
	
	public static String PARTITIONER = DSTR_PREFIX + "patitioner";
	
	/**
	 * {@link #STAGE} related configuration.<br>
	 * Provides a hint if map-side-combine should be attempted in a particular stage (e.g., <i>dstream.stage.ms_combine.0_ms=true</i>)<br>
	 * In the above '0_ms' identifies combination of 'stage id' + "_" + 'pipeline name' 
	 */
	public static String MAP_SIDE_COMBINE = DSTR_PREFIX + "ms_combine.";
}

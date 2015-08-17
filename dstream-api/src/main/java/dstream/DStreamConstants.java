/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream;

import java.net.URI;

import dstream.support.Classifier;

/**
 * Constant values used by the framework 
 */
public interface DStreamConstants {

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
	 * Identifies execution parallelism (e.g., <i>dstream.parallelism=3</i>)<br>
	 */
	public static String PARALLELISM = DSTR_PREFIX + "parallelism";
	
	/**
	 * Identifies the implementation of the {@link Classifier} (e.g., <i>dstream.grouper=dstream.function.</i>)<br>
	 */
	public static String CLASSIFIER = DSTR_PREFIX + "classifier";
	
	/**
	 * {@link #STAGE} related configuration.<br>
	 * Provides a hint if map-side-combine should be attempted in a particular stage (e.g., <i>dstream.ms_combine.0_wc=true</i>)<br>
	 * In the above '0_wc' identifies combination of 'stage id' + "_" + 'pipeline name' 
	 */
	public static String MAP_SIDE_COMBINE = DSTR_PREFIX + "ms_combine.";
}

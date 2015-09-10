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
package io.dstream;

import java.net.URI;

import io.dstream.support.Classifier;

/**
 * Constant values used by the framework
 */
public interface DStreamConstants {

	public static String DSTR_PREFIX = "dstream.";

	// Configuration constants
	/**
	 * Identifies the source {@link URI} of the individual pipeline.<br>
	 * For example:
	 * <pre>
	 * dstream.source.foo=hdfs://hadoop.com/demo/monte-cristo.txt
	 * </pre>
	 * In the above 'foo' would be the name of the pipeline
	 */
	public static String SOURCE = DSTR_PREFIX + "source.";


	public static String SOURCE_SUPPLIER = DSTR_PREFIX + "source_supplier.";

	/**
	 * Identifies the {@link DStreamExecutionDelegate} implementation used
	 * by a given execution.<br>
	 * For example:
	 * <pre>
	 * dstream.delegate=dstream.ValidationDelegate
	 * </pre>
	 */
	public static String DELEGATE = DSTR_PREFIX + "delegate";

	/**
	 * Identifies the output directory {@link URI} of the execution identified by
	 * name (e.g., <i>stream.executeAs("myExecution")</i>).<br>
	 * For example:
	 * <pre>
	 * dstream.output.myExecution=hdfs://hadoop.com/out
	 * </pre>
	 */
	public static String OUTPUT = DSTR_PREFIX + "output";

	/**
	 * Identifies execution parallelism size.<br>
	 * For example:
	 * <pre>
	 * dstream.parallelism=3
	 * </pre>
	 */
	public static String PARALLELISM = DSTR_PREFIX + "parallelism";

	/**
	 * Identifies the implementation of the {@link Classifier}.<br>
	 * For example:
	 * <pre>
	 * dstream.classifier=dstream.support.HashClassifier
	 * </pre>
	 */
	public static String CLASSIFIER = DSTR_PREFIX + "classifier";

	/**
	 * Provides a hint if map-side-combine should be attempted in a particular stage.<br>
	 * For example:
	 * <pre>
	 * dstream.ms_combine.0_wc=true
	 * </pre>
	 * In the above '0_wc' identifies combination of 'stage id' + "_" + 'pipeline name'
	 */
	public static String MAP_SIDE_COMBINE = DSTR_PREFIX + "ms_combine.";
}

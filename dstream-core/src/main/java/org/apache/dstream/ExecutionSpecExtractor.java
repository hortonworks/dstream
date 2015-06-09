package org.apache.dstream;

/**
 * Strategy to extract and array of {@link ExecutionSpec}s from its source.
 *
 */
interface ExecutionSpecExtractor {

	/**
	 * Extracts array of {@link ExecutionSpec} from its source.
	 * @return
	 */
	ExecutionSpec[] getExecutionSpec();
}

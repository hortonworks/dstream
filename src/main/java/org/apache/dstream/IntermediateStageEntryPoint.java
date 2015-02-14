package org.apache.dstream;

import org.apache.dstream.io.OutputSpecification;

/**
 * Strategy for defining an intermediate entry point.
 *
 * @param <T>
 */
public interface IntermediateStageEntryPoint<T> extends StageEntryPoint<T>{
	/**
	 * Will save the results of the intermediate computation to the disk based on provided {@link OutputSpecification}
	 * returning new {@link StreamExecutionContext}
	 * 
	 * @param outputSpec
	 * @return
	 */
	public StreamExecutionContext<T> saveAs(OutputSpecification outputSpec);
}
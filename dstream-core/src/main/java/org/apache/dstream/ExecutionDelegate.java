package org.apache.dstream;

import java.util.stream.Stream;

/**
 * 
 */
public interface ExecutionDelegate {

	/**
	 * Main delegation method between {@link DistributablePipelineSpecification} and its realization in the 
	 * target execution environment.
	 * 
	 * @param pipelineSpecification
	 * @return
	 */
	Stream<?>[] execute(DistributablePipelineSpecification pipelineSpecification);
	
	/**
	 * Returns close handler relevant to the execution context represented by this delegate
	 * @return
	 */
	Runnable getCloseHandler();
}

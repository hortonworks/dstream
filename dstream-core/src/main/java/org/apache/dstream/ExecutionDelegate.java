package org.apache.dstream;

import java.util.stream.Stream;

/**
 * 
 */
public interface ExecutionDelegate  {

	/**
	 * Main delegation method between {@link PipelineExecutionChain} and its realization in the 
	 * target execution environment.
	 * 
	 * @param pipelineSpecification
	 * @return
	 */
	Stream<Stream<?>>[] execute(PipelineExecutionChain... pipelineSpecification);

	/**
	 * Returns {@link Runnable} which contains logic relevant to 
	 * The returned {@link Runnable} will be executed when resulting {@link Stream#close()} is called.
	 * 
	 * @return
	 */
	Runnable getCloseHandler();
}

package org.apache.dstream;

import java.util.stream.Stream;

/**
 * 
 */
public interface ExecutionDelegate  {

	/**
	 * Main delegation operation to pass an array of {@link PipelineExecutionChain}s to 
	 * target execution environment.
	 * 
	 * @param pipelineSpecifications
	 * @return an array of Stream&lt;Stream&lt;?&gt;&gt; where each outer Stream represents 
	 * the result of execution of individual {@link PipelineExecutionChain}.<br>
	 * The result itself consists of {@link Stream}s representing each result partition. 
	 */
	Stream<Stream<?>>[] execute(PipelineExecutionChain... pipelineSpecifications);

	/**
	 * Returns {@link Runnable} which contains logic relevant to 
	 * The returned {@link Runnable} will be executed when resulting {@link Stream#close()} is called.
	 * 
	 * @return
	 */
	Runnable getCloseHandler();
}

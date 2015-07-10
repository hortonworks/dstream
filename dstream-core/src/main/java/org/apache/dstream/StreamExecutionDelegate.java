package org.apache.dstream;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Strategy to implement delegates to execute {@link DistributableStream} operations
 */
public interface StreamExecutionDelegate<T> {

	/**
	 * Main delegation operation to pass an array of {@link ExecutionSpec}s to 
	 * target execution environment.
	 * 
	 * @param executionName the name of this execution
	 * @param executionSpecs and array of {@link ExecutionSpec}s
	 * @return an array of {@link Stream}&lt;{@link Stream}&lt;?&gt;&gt; where each outer 
	 * {@link Stream} represents the result of execution of individual {@link ExecutionSpec}.<br>
	 * 
	 *  See {@link DistributableExecutable} for more details on the different result structures.
	 */
	// add comment that while signature allows for async invocation, the actual style coudl still be controlled by the implementation
	Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, OperationContext<T>... operationContexts);

	/**
	 * Returns {@link Runnable} which contains logic relevant to closing of the result {@link Stream}.
	 * The returned {@link Runnable} will be executed when resulting {@link Stream#close()} is called.
	 * 
	 * @return
	 */
	Runnable getCloseHandler();
}

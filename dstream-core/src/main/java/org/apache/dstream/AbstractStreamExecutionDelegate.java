package org.apache.dstream;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public abstract class AbstractStreamExecutionDelegate<T> implements StreamExecutionDelegate<T> {

	@SuppressWarnings("unchecked")
	@Override
	public Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, OperationContext<T>... operationContexts) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<Stream<Stream<?>>> resultFuture = executor.submit(new Callable<Stream<Stream<?>>>() {
				@Override
				public Stream<Stream<?>> call() throws Exception {
					try {
						return doExecute(executionName, executionConfig, operationContexts);
					} 
					catch (Exception e) {
						e.printStackTrace();
						throw new IllegalStateException(e);
					}
					finally {
						executor.shutdownNow();
					}
				}
			});
			return resultFuture;
		} catch (Exception e) {
			executor.shutdownNow();
			throw new IllegalStateException("Failed to execute stream", e);
		} 
	}

	@SuppressWarnings("unchecked")
	protected abstract Stream<Stream<?>> doExecute(String executionName, Properties executionConfig, OperationContext<T>... operationContexts);
	
	/**
	 * 
	 */
	protected boolean isIntermediateOperation(String operationName){
		return operationName.equals("flatMap") || 
			   operationName.equals("map") || 
			   operationName.equals("filter") ||
			   operationName.equals("compute");
	}
	
	/**
	 * 
	 */
	protected boolean isShuffleOperation(String operationName){
		return operationName.equals("group") ||
			   operationName.equals("reduceGroups") ||
			   operationName.equals("aggregateGroups") ||
			   operationName.equals("join") ||
			   operationName.equals("partition");
	}
	
	/**
	 * 
	 */
	protected boolean isTerminalOperation(String operationName){
		return operationName.equals("executeAs");
	}
}

package org.apache.dstream.support;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.StreamExecutionDelegate;
import org.apache.dstream.StreamInvocationChain;
import org.apache.dstream.utils.JvmUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public abstract class AbstractStreamExecutionDelegate<T> implements StreamExecutionDelegate<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, StreamInvocationChain... invocationChains) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<Stream<Stream<?>>> resultFuture = executor.submit(new Callable<Stream<Stream<?>>>() {
				@SuppressWarnings("unchecked")
				@Override
				public Stream<Stream<?>> call() throws Exception {
					try {
						Stream<Stream<?>> resultStreams = doExecute(executionName, executionConfig, invocationChains);
						return (Stream<Stream<?>>) mixinWithCloseHandler(resultStreams, new Runnable() {
									@Override
									public void run() {
										try {
											AbstractStreamExecutionDelegate.this.getCloseHandler().run();
										} 
										catch (Exception e) {
											logger.error("Failed during execution of close handler", e);
										} 
										finally {
											executor.shutdownNow();
										}
									}
								});
					} 
					catch (Exception e) {
						throw new IllegalStateException(e);
					}
					finally {
						executor.shutdownNow();
					}
				}
			});
			return resultFuture;
		} 
		catch (Exception e) {
			executor.shutdownNow();
			throw new IllegalStateException("Failed to execute stream", e);
		} 
	}

	/**
	 * 
	 * @param executionName
	 * @param executionConfig
	 * @param invocationChains
	 * @return
	 */
	protected abstract Stream<Stream<?>> doExecute(String executionName, Properties executionConfig, StreamInvocationChain... invocationChains);
	
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
	
	/**
	 * Creates proxy over the result Stream to ensures that close() call is always delegated to
	 * the close handler provided by the target ExecutionDelegate.
	 */
	private Stream<?> mixinWithCloseHandler(Stream<?> resultStream, Runnable closeHandler){
		resultStream.onClose(closeHandler);
		MethodInterceptor advice = new MethodInterceptor() {	
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				Object result = invocation.proceed();
				if (Stream.class.isAssignableFrom(invocation.getMethod().getReturnType())){
					Stream<?> stream = (Stream<?>) result;
					result = mixinWithCloseHandler(stream, closeHandler);
				}
				return result;
			}
		};
		return JvmUtils.proxy(resultStream, advice);
	}
}

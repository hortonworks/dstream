package org.apache.dstream;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.utils.JvmUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Implementation of {@link StreamExecutionDelegate}.
 * Primary use is testing.
 */
public class ValidationDelegate implements StreamExecutionDelegate<List<MethodInvocation>> {
	
	private final Logger logger = LoggerFactory.getLogger(ValidationDelegate.class);
	
	@Override
	public Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, OperationContext<List<MethodInvocation>>... operationContexts) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		CountDownLatch latch = new CountDownLatch(1);
		Future<Stream<Stream<?>>> result = executor.submit(new Callable<Stream<Stream<?>>>() {
			@SuppressWarnings("unchecked")
			@Override
			public Stream<Stream<?>> call() throws Exception {
				try {	
					Stream<?>[] results = Stream.of(operationContexts)
							.map(v -> operationContexts.length > 1 ? Stream.of(Stream.of(v)) : Stream.of(v))
							.collect(Collectors.toList()).toArray(new Stream[]{});
					
					Stream<Stream<?>> streamOfResults = (Stream<Stream<?>>) mixinWithCloseHandler(Stream.of(results), new Runnable() {
						@Override
						public void run() {
							try {
								getCloseHandler().run();
							} 
							catch (Exception e) {
								logger.error("Failed during execution of close handler", e);
							}
						}
					});

					return streamOfResults;
				} 
				catch (Exception e) {
					e.printStackTrace();
					throw new IllegalStateException(e);
				}
				finally {
					latch.countDown();
				}
			}
		});
		try {
			boolean success = latch.await(5000, TimeUnit.MILLISECONDS);
			if (!success){
				throw new IllegalStateException("Timedout waiting for latch");
			}
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		
		return result;
	}

	@Override
	public Runnable getCloseHandler() {
		return new Runnable() {
			@Override
			public void run() {
				logger.info("Executing close handler");
			}
		};
	}
	
	/**
	 * Creates proxy over the result Stream to ensures that close() call is always delegated to
	 * the close handler provided by the target ExecutionDelegate.
	 */
	private Stream<?> mixinWithCloseHandler(Stream<Stream<?>> resultStream, Runnable closeHandler){
		resultStream.onClose(closeHandler);
		MethodInterceptor advice = new MethodInterceptor() {	
			@SuppressWarnings("unchecked")
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				Object result = invocation.proceed();
				if (Stream.class.isAssignableFrom(invocation.getMethod().getReturnType())){
					Stream<Stream<?>> stream = (Stream<Stream<?>>) result;
					result = mixinWithCloseHandler(stream, closeHandler);
				}
				return result;
			}
		};
		return JvmUtils.proxy(resultStream, advice);
	}
}

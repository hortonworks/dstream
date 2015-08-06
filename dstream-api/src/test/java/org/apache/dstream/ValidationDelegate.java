package org.apache.dstream;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/**
 * Implementation of {@link StreamExecutionDelegate}.
 * Primary use is testing.
 */
public class ValidationDelegate implements StreamExecutionDelegate<StreamInvocationChain> {
	
	@Override
	public Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, StreamInvocationChain... invocationChains) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		CountDownLatch latch = new CountDownLatch(1);
		Future<Stream<Stream<?>>> result = executor.submit(new Callable<Stream<Stream<?>>>() {
			@SuppressWarnings("unchecked")
			@Override
			public Stream<Stream<?>> call() throws Exception {
				try {	
					Stream<?>[] results = Stream.of(invocationChains)
//							.map(v -> proxy(v))
							.map(v -> invocationChains.length > 1 ? Stream.of(Stream.of(v)) : Stream.of(v))
							.collect(Collectors.toList()).toArray(new Stream[]{});
					
					Stream<Stream<?>> streamOfResults = (Stream<Stream<?>>) mixinWithCloseHandler(Stream.of(results), new Runnable() {
						@Override
						public void run() {
							try {
								getCloseHandler().run();
							} 
							catch (Exception e) {
								e.printStackTrace();
//								logger.severe("Failed during execution of close handler");
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
		finally {
			executor.shutdownNow();
		}
		
		return result;
	}

	/**
	 * 
	 */
	@Override
	public Runnable getCloseHandler() {
		return new Runnable() {
			@Override
			public void run() {
//				logger.info("Executing close handler");
			}
		};
	}
	
	private Stream<?> mixinWithCloseHandler(Stream<?> resultStream, Runnable closeHandler){
		resultStream.onClose(closeHandler);
		InvocationHandler ih = new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				Object result = method.invoke(resultStream, args);
				if (Stream.class.isAssignableFrom(method.getReturnType())){
					Stream<?> stream = (Stream<?>) result;
					result = mixinWithCloseHandler(stream, closeHandler);
				}
				return result;
			}
		};
		return (Stream<?>) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{Stream.class}, ih);
	}
}

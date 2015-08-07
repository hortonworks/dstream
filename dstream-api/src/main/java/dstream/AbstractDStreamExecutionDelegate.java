package dstream;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * 
 * @param <T>
 */
public abstract class AbstractDStreamExecutionDelegate<T> implements DStreamExecutionDelegate<T> {
	@Override
	public Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, DStreamInvocationPipeline... invocationChains) {
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
											AbstractDStreamExecutionDelegate.this.getCloseHandler().run();
										} 
										catch (Exception e) {
											e.printStackTrace();
											throw new IllegalStateException("Failed during execution of close handler", e);
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
	protected abstract Stream<Stream<?>> doExecute(String executionName, Properties executionConfig, DStreamInvocationPipeline... invocationChains);
	
	/**
	 * Creates proxy over the result Stream to ensures that close() call is always delegated to
	 * the close handler provided by the target ExecutionDelegate.
	 */
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

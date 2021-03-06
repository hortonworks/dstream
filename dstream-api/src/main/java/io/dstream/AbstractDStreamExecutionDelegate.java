/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dstream;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Base implementation of {@link DStreamExecutionDelegate} which invokes
 * {@link #doExecute(String, Properties, DStreamExecutionGraph...)} asynchronously
 * and wraps every result {@link Stream} in a proxy allowing calls to {@link Stream#close()}
 * to be delegated to the the close handler returned by the {@link #getCloseHandler()}.
 */
public abstract class AbstractDStreamExecutionDelegate implements DStreamExecutionDelegate {
	/**
	 *
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, DStreamExecutionGraph... executionPipelines) {
		ExecutorService executor = Executors.newSingleThreadExecutor();

		try {
			Future<Stream<Stream<?>>> resultFuture = executor.submit(new Callable<Stream<Stream<?>>>() {
				@Override
				public Stream<Stream<?>> call() throws Exception {
					try {
						List<Stream<Stream<?>>> resultStreamsList = doExecute(executionName, executionConfig, executionPipelines);

						@SuppressWarnings("rawtypes")
						Stream resultStreams = resultStreamsList.size() == 1 ? resultStreamsList.get(0)
								: resultStreamsList.stream();

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
	 * A delegate operation invoked by {@link #execute(String, Properties, DStreamExecutionGraph...)} to
	 * be implemented by sub-classes.
	 *
	 * @param executionName the name of this execution
	 * @param executionConfig properties representing execution configuration
	 * @param executionGraphs execution graph
	 * @return List of result partition streams
	 */
	protected abstract List<Stream<Stream<?>>> doExecute(String executionName, Properties executionConfig, DStreamExecutionGraph... executionGraphs);

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

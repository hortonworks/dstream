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
package dstream;

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
 * Implementation of {@link DStreamExecutionDelegate}.
 * Primary use is testing.
 */
public class ValidationDelegate implements DStreamExecutionDelegate {
	
	@Override
	public Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, StreamOperations... operationsGroups) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		CountDownLatch latch = new CountDownLatch(1);
		Future<Stream<Stream<?>>> result = executor.submit(new Callable<Stream<Stream<?>>>() {
			@SuppressWarnings("unchecked")
			@Override
			public Stream<Stream<?>> call() throws Exception {
				try {	
					Stream<?>[] results = Stream.of(operationsGroups)
							.map(v -> operationsGroups.length > 1 ? Stream.of(Stream.of(v)) : Stream.of(v))
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

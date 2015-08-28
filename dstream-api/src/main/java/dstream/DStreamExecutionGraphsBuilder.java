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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.DStreamInvocationChain.DStreamInvocation;
import dstream.utils.Assert;
import dstream.utils.PropertiesHelper;
import dstream.utils.ReflectionUtils;

/**
 * 
 * @param <T>
 * @param <R>
 */
final class DStreamExecutionGraphsBuilder<T,R> {
	
	private final Logger logger  = Logger.getLogger(this.getClass().getName());

	private final R targetStream;
	
	private final DStreamInvocationChain invocationPipeline;
	
	private final Set<String> streamOperationNames;
	
	
	private final Class<?> currentStreamType;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param sourceProperty
	 * @param streamType
	 * @return
	 */
	static <T,R> R as(Class<T> sourceElementType, String sourceIdentifier, Class<R> streamType) {
		StreamNameMonitor.add(sourceIdentifier);
		@SuppressWarnings("unchecked")
		DStreamExecutionGraphsBuilder<T,R> builder = new DStreamExecutionGraphsBuilder<T,R>(sourceElementType, sourceIdentifier, streamType);
		return builder.targetStream;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private DStreamExecutionGraphsBuilder(Class<?> sourceElementType, String sourceIdentifier, Class<R>... streamType) {
		
		this.currentStreamType = streamType[0];
		this.targetStream =  this.generateStreamProxy(streamType);
		this.invocationPipeline = new DStreamInvocationChain(sourceElementType, sourceIdentifier, streamType[0]);
		this.streamOperationNames = ReflectionUtils.findAllVisibleMethodOnInterface(streamType[0]);
		this.streamOperationNames.remove("ofType");
		this.streamOperationNames.remove("executeAs");
		this.streamOperationNames.remove("getName");
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return this.invocationPipeline.getSourceIdentifier() + ":" + 
				this.invocationPipeline.getInvocations().stream().map(s -> s.getMethod().getName()).collect(Collectors.toList());
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private R cloneTargetDistributable(Method method, Object[] arguments){

		Ops operation = Ops.valueOf(method.getName());
		
		if (operation.equals(Ops.join) || operation.equals(Ops.union) || operation.equals(Ops.unionAll)){
			StreamInvocationChainSupplier s =  (StreamInvocationChainSupplier) arguments[0];
			arguments = new Object[]{s.get()};
		}

		DStreamExecutionGraphsBuilder clonedDistributable = new DStreamExecutionGraphsBuilder(this.invocationPipeline.getSourceElementType(), this.invocationPipeline.getSourceIdentifier(), 
				method.getReturnType().isInterface() ? method.getReturnType() : this.currentStreamType);	
		clonedDistributable.invocationPipeline.addAllInvocations(this.invocationPipeline.getInvocations());	
		if (operation.equals(Ops.on)){
			clonedDistributable.invocationPipeline.getLastInvocation().setSupplementaryOperation(arguments[0]);
		}
		else {
			clonedDistributable.invocationPipeline.addInvocation(new DStreamInvocation(method, arguments));
		}
		return (R) clonedDistributable.targetStream;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private R generateStreamProxy(Class<?>... proxyTypes){	
		List<Class<?>> interfaces = Stream.of(proxyTypes).collect(Collectors.toList());
		interfaces.add(StreamInvocationChainSupplier.class);
		return (R) Proxy.newProxyInstance(this.getClass().getClassLoader(), interfaces.toArray(new Class[]{}), new StreamInvocationHandler());
	}
	
	/**
	 * 
	 */
	private Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		String operationName = method.getName();
		
		Object result;
		if (this.streamOperationNames.contains(operationName)){
			logger.info("Invoking OPERATION: " + operationName);
			result = this.cloneTargetDistributable(method, args == null ? new Object[]{} : args);
		}
		else if (operationName.equals("getName")){
			result = this.invocationPipeline.getSourceIdentifier();
		}
		else if (operationName.equals("get")){
			result = this.invocationPipeline;
		}
		else if (operationName.equals("executeAs")){
			StreamNameMonitor.reset();
			String executionName = (String) args[0];
			Assert.notEmpty(executionName, "'executionName' must not be null or empty");
	
			Properties executionConfig = PropertiesHelper.loadProperties(executionName + ".cfg");
			String executionDelegateClassName = executionConfig.getProperty(DStreamConstants.DELEGATE);
			Assert.notEmpty(executionDelegateClassName, "Execution delegate property is not provided in '" + executionName + 
					".cfg' (e.g., dstream.delegate=foo.bar.SomePipelineDelegate)");
			
			logger.info("Delegating execution to: " + executionDelegateClassName);
					
			DStreamExecutionGraphBuilder builder = new DStreamExecutionGraphBuilder(this.invocationPipeline, executionConfig);
			
			DStreamExecutionGraph operations = builder.build();
			
			DStreamExecutionDelegate executionDelegate = (DStreamExecutionDelegate) ReflectionUtils
					.newDefaultInstance(Class.forName(executionDelegateClassName, true, Thread.currentThread().getContextClassLoader()));
			
			result = executionDelegate.execute(executionName, executionConfig, operations);
		}
		else {
			result = method.invoke(this, args);
		}
		
		return result;
	}
	
	/**
	 */
	private static interface StreamInvocationChainSupplier {
		DStreamInvocationChain get();
	}
	
	/**
	 *
	 */
	private class StreamInvocationHandler implements InvocationHandler{

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			return DStreamExecutionGraphsBuilder.this.invoke(proxy, method, args);
		}
	}
	
	/**
	 * 
	 */
	static class StreamNameMonitor {
		private final static ThreadLocal<Set<String>> tl = ThreadLocal.withInitial(() -> new HashSet<String>());
		
		static void add(String name){
			Assert.isFalse(tl.get().contains(name), "Stream with the name '" + name + "' already exists");
			tl.get().add(name);
		}
		
		static void reset(){
			tl.get().clear();
		}
	}
}
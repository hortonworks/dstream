package org.apache.dstream;

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

import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.PropertiesHelper;
import org.apache.dstream.utils.ReflectionUtils;

/**
 * 
 * @param <T>
 * @param <R>
 */
final class DStreamInvocationPipelineBuilder<T,R> {
	
	private final Logger logger  = Logger.getLogger(this.getClass().getName());

	private final R targetStream;
	
	private final DStreamInvocationPipeline invocatioinPipeline;
	
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
		DStreamInvocationPipelineBuilder<T,R> builder = new DStreamInvocationPipelineBuilder<T,R>(sourceElementType, sourceIdentifier, streamType);
		return builder.targetStream;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private DStreamInvocationPipelineBuilder(Class<?> sourceElementType, String sourceIdentifier, Class<R>... streamType) {
		
		this.currentStreamType = streamType[0];
		this.targetStream =  this.generateStreamProxy(streamType);
		this.invocatioinPipeline = new DStreamInvocationPipeline(sourceElementType, sourceIdentifier, streamType[0]);
		this.streamOperationNames = ReflectionUtils.findAllVisibleMethodOnInterface(streamType[0]);
		this.streamOperationNames.remove("ofType");
		this.streamOperationNames.remove("executeAs");
		this.streamOperationNames.remove("getSourceIdentifier");
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return this.invocatioinPipeline.getSourceIdentifier() + ":" + 
				this.invocatioinPipeline.getInvocations().stream().map(s -> s.getMethod().getName()).collect(Collectors.toList());
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private R cloneTargetDistributable(Method method, Object[] arguments){
		String operationName = method.getName();

		if (operationName.equals("join") || operationName.startsWith("union")){
			StreamInvocationChainSupplier s =  (StreamInvocationChainSupplier) arguments[0];
			arguments = new Object[]{s.get()};
		}

		DStreamInvocationPipelineBuilder clonedDistributable = new DStreamInvocationPipelineBuilder(this.invocatioinPipeline.getSourceElementType(), this.invocatioinPipeline.getSourceIdentifier(), 
				method.getReturnType().isInterface() ? method.getReturnType() : this.currentStreamType);	
		clonedDistributable.invocatioinPipeline.addAllInvocations(this.invocatioinPipeline.getInvocations());	
		clonedDistributable.invocatioinPipeline.addInvocation(new DStreamInvocation(method, arguments));
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
	
	@SuppressWarnings("unchecked")
	private Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		String operationName = method.getName();
		Object result;
		if (this.streamOperationNames.contains(operationName)){
			logger.info("Invoking OPERATION: " + operationName);
			result = this.cloneTargetDistributable(method, args == null ? new Object[]{} : args);
		}
		else if (operationName.equals("getSourceIdentifier")){
			result = this.invocatioinPipeline.getSourceIdentifier();
		}
		else if (operationName.equals("get")){
			result = this.invocatioinPipeline;
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
			
			DStreamExecutionDelegate<List<DStreamInvocation>> executionDelegate = (DStreamExecutionDelegate<List<DStreamInvocation>>) ReflectionUtils
					.newDefaultInstance(Class.forName(executionDelegateClassName, true, Thread.currentThread().getContextClassLoader()));
			
			result = executionDelegate.execute(executionName, executionConfig, this.invocatioinPipeline);
		}
		else {
			result = method.invoke(this, args);
		}
		
		return result;
	}
	
	/**
	 */
	private static interface StreamInvocationChainSupplier {
		DStreamInvocationPipeline get();
	}
	
	/**
	 *
	 */
	private class StreamInvocationHandler implements InvocationHandler{

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			return DStreamInvocationPipelineBuilder.this.invoke(proxy, method, args);
		}
	}
	
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
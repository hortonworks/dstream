package org.apache.dstream;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
final class DStreamOperationsCollector<T,R> {

	private final R targetStream;
	
	private final StreamInvocationChain invocationChain;
	
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
		@SuppressWarnings("unchecked")
		DStreamOperationsCollector<T,R> builder = new DStreamOperationsCollector<T,R>(sourceElementType, sourceIdentifier, streamType);
		return builder.targetStream;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private DStreamOperationsCollector(Class<?> sourceElementType, String sourceIdentifier, Class<R>... streamType) {
		this.currentStreamType = streamType[0];
		this.targetStream =  this.generateStreamProxy(streamType);
		this.invocationChain = new StreamInvocationChain(sourceElementType, sourceIdentifier, streamType[0]);
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
		return this.invocationChain.getSourceIdentifier() + ":" + 
				this.invocationChain.getInvocations().stream().map(s -> s.getMethod().getName()).collect(Collectors.toList());
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

		DStreamOperationsCollector clonedDistributable = new DStreamOperationsCollector(this.invocationChain.getSourceElementType(), this.invocationChain.getSourceIdentifier(), 
				method.getReturnType().isInterface() ? method.getReturnType() : this.currentStreamType);	
		clonedDistributable.invocationChain.addAllInvocations(this.invocationChain.getInvocations());	
		clonedDistributable.invocationChain.addInvocation(new APIInvocation(method, arguments));
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
//			logger.info("OPERATION: " + operationName + " - " + method.getReturnType());
			result = this.cloneTargetDistributable(method, args == null ? new Object[]{} : args);
		}
		else if (operationName.equals("getSourceIdentifier")){
			result = this.invocationChain.getSourceIdentifier();
		}
		else if (operationName.equals("get")){
			result = this.invocationChain;
		}
		else if (operationName.equals("executeAs")){
			StreamNameMonitor.reset();
			String executionName = (String) args[0];
			Assert.notEmpty(executionName, "'executionName' must not be null or empty");
	
			Properties executionConfig = PropertiesHelper.loadProperties(executionName + ".cfg");
			String executionDelegateClassName = executionConfig.getProperty(DistributableConstants.DELEGATE);
			Assert.notEmpty(executionDelegateClassName, "Execution delegate property is not provided in '" + executionName + 
					".cfg' (e.g., dstream.delegate=foo.bar.SomePipelineDelegate)");
			
//			logger.info("Execution delegate: " + executionDelegateClassName);
			
			StreamExecutionDelegate<List<APIInvocation>> executionDelegate = (StreamExecutionDelegate<List<APIInvocation>>) ReflectionUtils
					.newDefaultInstance(Class.forName(executionDelegateClassName, true, Thread.currentThread().getContextClassLoader()));
			
			result = executionDelegate.execute(executionName, executionConfig, this.invocationChain);
		}
		else {
			result = method.invoke(this, args);
		}
		
		return result;
	}
	
	/**
	 */
	private static interface StreamInvocationChainSupplier {
		StreamInvocationChain get();
	}
	
	/**
	 *
	 */
	private class StreamInvocationHandler implements InvocationHandler{

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			return DStreamOperationsCollector.this.invoke(proxy, method, args);
		}
	}
}
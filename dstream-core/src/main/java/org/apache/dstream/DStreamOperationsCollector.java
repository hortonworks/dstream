package org.apache.dstream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.PropertiesHelper;
import org.apache.dstream.utils.JvmUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

/**
 * 
 * @param <T>
 * @param <R>
 */
final class DStreamOperationsCollector<T,R> implements MethodInterceptor {

	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	private final R targetStream;
	
	private final StreamInvocationChain invocationChain;
	
	private final Set<String> streamOperationNames;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param sourceProperty
	 * @param streamType
	 * @return
	 */
	static <T,R> R as(Class<T> sourceElementType, String sourceIdentifier, Class<R> streamType) {
		DStreamOperationsCollector<T,R> builder = new DStreamOperationsCollector<T,R>(sourceElementType, sourceIdentifier, streamType);
		return builder.targetStream;
	}
	
	/**
	 * 
	 */
	private DStreamOperationsCollector(Class<?> sourceElementType, String sourceIdentifier, Class<R> streamType) {
		this.targetStream =  this.generateStreamProxy(streamType);
		this.invocationChain = new StreamInvocationChain(sourceElementType, sourceIdentifier, streamType);
		this.streamOperationNames = Stream.of(DStream.class.getDeclaredMethods()).map(s -> s.getName()).collect(Collectors.toSet());
		this.streamOperationNames.add("on");
		this.streamOperationNames.add("where");
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		String operationName = invocation.getMethod().getName();
		Object result;
		if (this.streamOperationNames.contains(operationName)){
			if (logger.isDebugEnabled()){
				logger.debug("OPERATION: " + operationName + " - " + invocation.getMethod().getReturnType());
			}
			result = this.cloneTargetDistributable(invocation);
		}
		else if (operationName.equals("getSourceIdentifier")){
			result = this.invocationChain.getSourceIdentifier();
		}
		else if (operationName.equals("get")){
			result = this.invocationChain;
		}
		else if (operationName.equals("executeAs")){
			String executionName = (String) invocation.getArguments()[0];
			Assert.notEmpty(executionName, "'executionName' must not be null or empty");
	
			Properties executionConfig = PropertiesHelper.loadProperties(executionName + ".cfg");
			String executionDelegateClassName = executionConfig.getProperty(DistributableConstants.DELEGATE);
			Assert.notEmpty(executionDelegateClassName, "Execution delegate property is not provided in '" + executionName + 
					".cfg' (e.g., dstream.delegate=foo.bar.SomePipelineDelegate)");
			
			if (logger.isInfoEnabled()) {
				logger.info("Execution delegate: " + executionDelegateClassName);
			}
			
			StreamExecutionDelegate<List<MethodInvocation>> executionDelegate = (StreamExecutionDelegate<List<MethodInvocation>>) ReflectionUtils
					.newDefaultInstance(Class.forName(executionDelegateClassName, true, Thread.currentThread().getContextClassLoader()));
			
			result = executionDelegate.execute(executionName, executionConfig, this.invocationChain);
		}
		else {
			result = invocation.proceed();
		}
		
		return result;
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
	private R cloneTargetDistributable(MethodInvocation invocation){
		String operationName = invocation.getMethod().getName();

		if (operationName.equals("join")){
			ProxyInternalsAccessor<StreamInvocationChain> invocationChainAccessor = (ProxyInternalsAccessor<StreamInvocationChain>) invocation.getArguments()[0];
			StreamInvocationChain dependentInvocationChain = invocationChainAccessor.get();
			((ReflectiveMethodInvocation)invocation).setArguments(new Object[]{dependentInvocationChain});
		}
			
		DStreamOperationsCollector clonedDistributable = new DStreamOperationsCollector(this.invocationChain.getSourceElementType(), this.invocationChain.getSourceIdentifier(), 
				invocation.getMethod().getReturnType());	
		clonedDistributable.invocationChain.addAllInvocations(this.invocationChain.getInvocations());	
		clonedDistributable.invocationChain.addInvocation(invocation);
		return (R) clonedDistributable.targetStream;
	}
	
	/**
	 * 
	 */
	private R generateStreamProxy(Class<?> proxyType){	
		List<Class<?>> interfaces = new ArrayList<Class<?>>();
		interfaces.add(proxyType);
		interfaces.add(ProxyInternalsAccessor.class);

		R streamProxy = JvmUtils.proxy(this, this, interfaces.toArray(new Class[]{}));
		if (logger.isDebugEnabled()){
			logger.debug("Constructed builder proxy for " + interfaces);
		}
		return streamProxy;
	}
	
	/**
	 * @param <T>
	 */
	protected interface ProxyInternalsAccessor<T>{
		T get();
	}
}
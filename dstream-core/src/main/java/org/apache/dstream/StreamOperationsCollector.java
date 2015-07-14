package org.apache.dstream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.support.PipelineConfigurationHelper;
import org.apache.dstream.utils.Assert;
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
final class StreamOperationsCollector<T,R extends DistributableStream<?>> implements MethodInterceptor {

	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	private final R targetStream;
	
	private final List<MethodInvocation> operations;
	
	private final Class<?> sourceElementType;
	
	private final String sourceIdentifier;
	
	private final Set<String> streamOperationNames;
	
	private final Class<R> streamType;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param sourceProperty
	 * @param streamType
	 * @return
	 */
	static <T,R extends DistributableStream<T>> R as(Class<T> sourceElementType, String sourceIdentifier, Class<R> streamType) {
		StreamOperationsCollector<T,R> builder = new StreamOperationsCollector<T,R>(sourceElementType, sourceIdentifier, streamType);
		return builder.targetStream;
	}
	
	/**
	 * 
	 */
	private StreamOperationsCollector(Class<?> sourceElementType, String sourceIdentifier, Class<R> streamType) {
		this.sourceElementType = sourceElementType;
		this.sourceIdentifier = sourceIdentifier;
		this.streamType = streamType;
		this.targetStream =  this.generateStreamProxy(streamType);
		this.operations = new ArrayList<>();
		this.streamOperationNames = Stream.of(DistributableStream.class.getDeclaredMethods()).map(s -> s.getName()).collect(Collectors.toSet());
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		((ReflectiveMethodInvocation)invocation).setUserAttribute("sourceElementType", sourceElementType);
		((ReflectiveMethodInvocation)invocation).setUserAttribute("sourceIdentifier", sourceIdentifier);
		
		String operationName = invocation.getMethod().getName();
		Object result;
		if (this.streamOperationNames.contains(operationName)){
			result = this.cloneTargetDistributable(invocation);
		}
		else if (operationName.equals("getSourceIdentifier")){
			result = this.sourceIdentifier;
		}
		else if (operationName.equals("executeAs")){
			String executionName = (String) invocation.getArguments()[0];
			Assert.notEmpty(executionName, "'executionName' must not be null or empty");
	
			Properties executionConfig = PipelineConfigurationHelper.loadExecutionConfig(executionName);
			String executionDelegateClassName = executionConfig.getProperty(DistributableConstants.DELEGATE);
			Assert.notEmpty(executionDelegateClassName, "Execution delegate for '" + executionName
					+ "' is not provided in 'execution-delegates.cfg' (e.g., " + executionName + "=foo.bar.SomePipelineDelegate)");
			
			if (logger.isInfoEnabled()) {
				logger.info("Execution delegate: " + executionDelegateClassName);
			}
			
			StreamExecutionDelegate<List<MethodInvocation>> executionDelegate = (StreamExecutionDelegate<List<MethodInvocation>>) ReflectionUtils
					.newDefaultInstance(Class.forName(executionDelegateClassName, true, Thread.currentThread().getContextClassLoader()));
			
			result = this.doExecute(executionName, executionConfig, executionDelegate);
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
		return this.sourceIdentifier + ":" + this.operations.stream().map(s -> s.getMethod().getName()).collect(Collectors.toList());
	}

	/**
	 * Will add {@link OperationContext} interface to the proxy delegating it's invocation to the 
	 * list of operations of this stream. The list of operations will be used by 
	 * target execution environment ( {@link StreamExecutionDelegate} ) to execute the stream.
	 */
	@SuppressWarnings("unchecked")
	private Object doExecute(String executionName, Properties executionConfig, StreamExecutionDelegate<List<MethodInvocation>> executionDelegate) {	
		OperationContext<List<MethodInvocation>> invocationChain = JvmUtils.proxy(this, new MethodInterceptor() {		
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				return invocation.getMethod().getName().equals("get") 
						? StreamOperationsCollector.this.operations 
								: invocation.proceed();
			}
		}, OperationContext.class);
		
		return executionDelegate.execute(executionName, executionConfig, invocationChain);
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private R cloneTargetDistributable(MethodInvocation invocation){
		StreamOperationsCollector clonedDistributable = new StreamOperationsCollector(null, this.sourceIdentifier, this.streamType);	
		clonedDistributable.operations.addAll(this.operations);	
		clonedDistributable.operations.add(invocation);
		return (R) clonedDistributable.targetStream;
	}
	
	/**
	 * 
	 */
	private R generateStreamProxy(Class<?> proxyType){	
		List<Class<?>> interfaces = new ArrayList<Class<?>>();
		if (DistributableStream.class.isAssignableFrom(proxyType)){
			interfaces.add(DistributableStream.class);
		}
		else if (ExecutionGroup.class.isAssignableFrom(proxyType)){
			interfaces.add(ExecutionGroup.class);
		}
		else {
			throw new IllegalArgumentException("Unsupported proxy type: " +  proxyType);
		}
		
		R streamProxy = JvmUtils.proxy(this, this, interfaces.toArray(new Class[]{}));
		if (logger.isDebugEnabled()){
			logger.debug("Constructed builder proxy for " + interfaces);
		}
		return streamProxy;
	}
}
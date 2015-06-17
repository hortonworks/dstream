package org.apache.dstream;

import java.util.ArrayList;
import java.util.List;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.JvmUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for implementing alternative execution specification builders to build
 * and execute instances of {@link DistributableExecutable} such as:.<br>
 * 		- {@link DistributablePipeline}<br>
 * 		- {@link DistributableStream}<br>
 * 		- {@link ExecutionGroup}.<br>
 * 
 * It creates a proxy allowing all method invocations to be funneled through 
 * {@link #invoke(MethodInvocation)} method which delegates to the abstract 
 * {@link #doInvoke(MethodInvocation)} method, which must be implemented by the sub-class.
 * 
 * @param <T> the type of the elements in the pipeline
 * @param <R> the type of {@link DistributableExecutable}.
 */
public abstract class AbstractExecutionSpecBuilder<T,R> implements MethodInterceptor {

	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	protected final R targetDistributable;
	
	protected final Class<?> sourceItemType;
	
	protected final String pipelineName;
	
	protected AbstractExecutionSpecBuilder(Class<?> sourceItemType, String pipelineName, Class<R> distributableType) {	
		Assert.isTrue(DistributableStream.class.isAssignableFrom(distributableType) 
				|| DistributablePipeline.class.isAssignableFrom(distributableType)
				|| ExecutionGroup.class.isAssignableFrom(distributableType), "Unsupported 'DistributableExecutable' type " + 
						distributableType + ". Supported types are " + DistributablePipeline.class + " & " + DistributableStream.class);

		this.targetDistributable =  this.generateDistributable(distributableType);
		this.sourceItemType = sourceItemType;
		this.pipelineName = pipelineName;
	}
	
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		return this.doInvoke(invocation);
	}
	
	/**
	 * Provides access to all method invocations on any instance of {@link DistributableExecutable}
	 * 
	 * @param invocation
	 * @return
	 * @throws Throwable
	 */
	protected abstract Object doInvoke(MethodInvocation invocation) throws Throwable;
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private R generateDistributable(Class<?> proxyType){	
		List<Class<?>> interfaces = new ArrayList<Class<?>>();
		if (DistributablePipeline.class.isAssignableFrom(proxyType)){
			interfaces.add(DistributablePipeline.class);
		} 
		else if (DistributableStream.class.isAssignableFrom(proxyType)){
			interfaces.add(DistributableStream.class);
		}
		else if (ExecutionGroup.class.isAssignableFrom(proxyType)){
			interfaces.add(ExecutionGroup.class);
		}
		else {
			throw new IllegalArgumentException("Unsupported proxy type: " +  proxyType);
		}
		interfaces.add(ExecutionConfigGenerator.class);
		
		R builderProxy = JvmUtils.proxy(new ArrayList(), this, interfaces.toArray(new Class[]{}));
		if (logger.isDebugEnabled()){
			logger.debug("Constructed builder proxy for " + interfaces);
		}
		return builderProxy;
	}
	
	/**
	 * 
	 */
	protected boolean isStageOperation(String operationName){
		return this.isStreamStageOperation(operationName) ||
			   operationName.equals("compute");
	}
	
	/**
	 * 
	 */
	protected boolean isStreamStageOperation(String operationName){
		return operationName.equals("flatMap") || 
			   operationName.equals("map") || 
			   operationName.equals("filter");
	}
	
	/**
	 * 
	 */
	protected boolean isShuffleOperation(String operationName){
		return operationName.equals("combine") ||
			   operationName.equals("group") ||
			   operationName.equals("join") ||
			   operationName.equals("split");
	}
}

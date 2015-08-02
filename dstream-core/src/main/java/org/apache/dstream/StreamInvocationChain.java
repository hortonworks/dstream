package org.apache.dstream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.aopalliance.intercept.MethodInvocation;

/**
 * 
 *
 */
public final class StreamInvocationChain {

	private final List<MethodInvocation> invocations;

	private final Class<?> sourceElementType;
	
	private final String sourceIdentifier;
	
	private final Class<?> streamType;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param sourceIdentifier
	 */
	protected StreamInvocationChain(Class<?> sourceElementType, String sourceIdentifier, Class<?> streamType){
		this.sourceElementType = sourceElementType;
		this.sourceIdentifier = sourceIdentifier;
		this.invocations = new ArrayList<>();
		this.streamType = streamType;
	}
	
	public Class<?> getStreamType() {
		return streamType;
	}

	
	/**
	 * 
	 * @return
	 */
	public List<MethodInvocation> getInvocations() {
		return Collections.unmodifiableList(this.invocations);
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getSourceElementType() {
		return sourceElementType;
	}

	/**
	 * 
	 * @return
	 */
	public String getSourceIdentifier() {
		return sourceIdentifier;
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addInvocation(MethodInvocation invocation){
		this.invocations.add(invocation);
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addAllInvocations(List<MethodInvocation> invocations){
		this.invocations.addAll(invocations);
	}
}

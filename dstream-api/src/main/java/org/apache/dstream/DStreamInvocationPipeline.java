package org.apache.dstream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 
 *
 */
public final class DStreamInvocationPipeline {

	private final List<DStreamInvocation> invocations;

	private final Class<?> sourceElementType;
	
	private final String sourceIdentifier;
	
	private final Class<?> streamType;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param sourceIdentifier
	 * @param streamType
	 */
	protected DStreamInvocationPipeline(Class<?> sourceElementType, String sourceIdentifier, Class<?> streamType){
		this.sourceElementType = sourceElementType;
		this.sourceIdentifier = sourceIdentifier;
		this.invocations = new ArrayList<>();
		this.streamType = streamType;
	}
	
	/**
	 * 
	 * @return
	 */
	public Class<?> getStreamType() {
		return this.streamType;
	}

	/**
	 * 
	 * @return
	 */
	public List<DStreamInvocation> getInvocations() {
		return Collections.unmodifiableList(this.invocations);
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getSourceElementType() {
		return this.sourceElementType;
	}

	/**
	 * 
	 * @return
	 */
	public String getSourceIdentifier() {
		return this.sourceIdentifier;
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addInvocation(DStreamInvocation invocation){
		this.invocations.add(invocation);
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addAllInvocations(List<DStreamInvocation> invocations){
		this.invocations.addAll(invocations);
	}
}

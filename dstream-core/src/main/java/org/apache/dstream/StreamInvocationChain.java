package org.apache.dstream;

import java.util.ArrayList;
import java.util.List;

import org.aopalliance.intercept.MethodInvocation;

public class StreamInvocationChain {

	private final List<MethodInvocation> invocations;

	private final Class<?> sourceElementType;
	
	private final String sourceIdentifier;
	
	protected StreamInvocationChain(Class<?> sourceElementType, String sourceIdentifier){
		this.sourceElementType = sourceElementType;
		this.sourceIdentifier = sourceIdentifier;
		this.invocations = new ArrayList<>();
	}
	
	public List<MethodInvocation> getInvocations() {
		return invocations;
	}

	public Class<?> getSourceElementType() {
		return sourceElementType;
	}

	public String getSourceIdentifier() {
		return sourceIdentifier;
	}
	
	public void addInvocation(MethodInvocation invocation){
		this.invocations.add(invocation);
	}
}

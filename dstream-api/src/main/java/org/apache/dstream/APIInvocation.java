package org.apache.dstream;

import java.lang.reflect.Method;

public final class APIInvocation {
	
	private final Method method;

	private final Object[] arguments;

	APIInvocation(Method method, Object... arguments){
		this.method = method;
		this.arguments = arguments;
	}
	
	public Method getMethod() {
		return method;
	}

	public Object[] getArguments() {
		return arguments;
	}
}

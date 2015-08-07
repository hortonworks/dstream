package dstream;

import java.lang.reflect.Method;

public final class DStreamInvocation {
	
	private final Method method;

	private final Object[] arguments;

	DStreamInvocation(Method method, Object... arguments){
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

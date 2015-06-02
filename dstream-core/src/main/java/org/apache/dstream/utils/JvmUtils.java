package org.apache.dstream.utils;

import java.lang.reflect.Constructor;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;

import sun.misc.Unsafe;

@SuppressWarnings("rawtypes")
public class JvmUtils {
	
	private static Unsafe unsafe;
	
	static {
		try {
			Constructor ctr = Unsafe.class.getDeclaredConstructor();
			ctr.setAccessible(true);
			unsafe = (Unsafe) ctr.newInstance();
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create Unsafe", e);
		}
	}
	
	public static Unsafe getUnsafe(){
		return unsafe;
	}

	@SuppressWarnings("unchecked")
	public static <T> T createDummyInstance(Class<T> clazz) {	
		try {
			if (clazz.isInterface()){
				ProxyFactory pf = new ProxyFactory();
				pf.addAdvice(new MethodInterceptor() {
					@Override
					public Object invoke(MethodInvocation invocation) throws Throwable {
						return null;
					}
				});
				pf.addInterface(clazz);
				return (T) pf.getProxy();
			}
			else {
				return (T) unsafe.allocateInstance(clazz);
			}
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create instance of " + clazz, e);
		}
	}
}

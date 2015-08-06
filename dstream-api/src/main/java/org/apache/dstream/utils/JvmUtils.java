package org.apache.dstream.utils;

import java.lang.reflect.Constructor;
import java.util.stream.Stream;

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
	
	/**
	 * 
	 * @return
	 */
	public static Unsafe getUnsafe(){
		return unsafe;
	}

	/**
	 * 
	 * @param clazz
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T createDummyInstance(Class<T> clazz) {	
		try {
			if (clazz.isInterface()){
				return proxy(null, new MethodInterceptor() {
					@Override
					public Object invoke(MethodInvocation invocation) throws Throwable {
						return null;
					}
				}, clazz);
			}
			else {
				return (T) unsafe.allocateInstance(clazz);
			}
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create instance of " + clazz, e);
		}
	}
	
	/**
	 * 
	 * @param target
	 * @param advice
	 * @param interfaces
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T proxy(Object target, MethodInterceptor advice, Class<?>... interfaces){
		ProxyFactory pf = target == null ? new ProxyFactory() : new ProxyFactory(target);
		if (interfaces != null){
			Stream.of(interfaces).forEach(pf::addInterface);
		}
		pf.addAdvice(advice);
		return (T) pf.getProxy();
	}
}

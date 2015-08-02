package org.apache.dstream.instrument;

import instrument.ClassLoaderInterceptor;

import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

public class TupleClassLoaderInterceptor implements ClassLoaderInterceptor {
	
	public TupleClassLoaderInterceptor(){
		//-javaagent:/Users/ozhurakousky/dev/lunaworkspace/instrument/instrument.jar
		TupleEnhancer.enhance();
	}
	
	@Override
	public byte[] transform(ClassLoader loader, String className,
			Class<?> classBeingRedefined, ProtectionDomain protectionDomain,
			byte[] classfileBuffer) throws IllegalClassFormatException {	
		return null;
	}
}

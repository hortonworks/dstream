package org.apache.dstream.instrument;

import java.util.Iterator;
import java.util.Properties;

import javassist.ClassPool;
import javassist.CtClass;

import org.apache.dstream.utils.PropertiesHelper;
import org.springframework.util.Assert;

/**
 * 
 */
public class TupleEnhancer {

	/**
	 * 
	 */
	public static void enhance(){
		try {
			ClassPool classPool = ClassPool.getDefault();
			CtClass tupleCtClass = classPool.get("org.apache.dstream.utils.Tuples$Tuple");
			Properties tupleDefs = PropertiesHelper.loadProperties("tuples.def");
			Iterator<Object> iter = tupleDefs.keySet().iterator();
			while (iter.hasNext()){
				String tupleClassName = (String) iter.next();
				CtClass tc = classPool.get(tupleClassName);
				Assert.isTrue(tc.isInterface(), tc.getName() + " must be an interface");
				System.out.println("Adding: " + tc.getName());
				tupleCtClass.addInterface(tc);
			}
			classPool.toClass(tupleCtClass);
		} 
		catch (Exception e) {
			// should never happen
			throw new IllegalStateException("Failed to load org.apache.dstream.utils.Tuples.Tuple", e);
		}
	}
}

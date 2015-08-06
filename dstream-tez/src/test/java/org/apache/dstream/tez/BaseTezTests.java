package org.apache.dstream.tez;

import java.io.File;
import java.lang.reflect.Constructor;

import org.apache.commons.io.FileUtils;

import sun.misc.Unsafe;

@SuppressWarnings("rawtypes")
public class BaseTezTests {
	
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

	public static void clean(String applicationName){
		try {
			File workDir = new File(System.getProperty("user.dir"));
			if (workDir.isDirectory()){
				for (String sub : workDir.list()) {
					if (sub.startsWith(applicationName)){
						File file = new File(workDir, sub);
						FileUtils.deleteDirectory(file);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected static Unsafe getUnsafe(){
		return unsafe;
	}
}

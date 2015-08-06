package org.apache.dstream;

import java.util.HashSet;
import java.util.Set;

import org.apache.dstream.utils.Assert;

class StreamNameMonitor {
	private final static ThreadLocal<Set<String>> tl = ThreadLocal.withInitial(() -> new HashSet<String>());
	
	static void add(String name){
		Assert.isFalse(tl.get().contains(name), "Stream with the name '" + name + "' already exists");
		tl.get().add(name);
	}
	
	static void reset(){
		tl.get().clear();
	}
}

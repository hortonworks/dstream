package org.apache.dstream.support;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CollectionFactory {

	<T> List<T> newList();
	
	<K,V> Map<K,V> newMap();
	
	<T> Set<T> newSet();
}

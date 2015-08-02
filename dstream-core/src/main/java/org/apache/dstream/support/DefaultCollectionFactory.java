package org.apache.dstream.support;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultCollectionFactory implements CollectionFactory {

	@Override
	public <T> List<T> newList() {
		return new ArrayList<T>();
	}

	@Override
	public <K, V> Map<K, V> newMap() {
		return new HashMap<K, V>();
	}

	@Override
	public <T> Set<T> newSet() {
		return new HashSet<>();
	}
}

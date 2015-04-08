package org.apache.dstream.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.utils.Utils;

class MultiValueMap<K,V> extends HashMap<K, List<V>> {
	private static final long serialVersionUID = -6738737584525785087L;

	public void add(K key, V value) {
		List<V> val = this.get(key);
		if (val == null){
			val = new ArrayList<V>();
			this.put(key, val);
		}
		val.add(value);
	}
	
	public void addEntry(Entry<K, V> entry) {
		List<V> val = this.get(entry.getKey());
		if (val == null){
			val = new ArrayList<V>();
			this.put(entry.getKey(), val);
		}
		val.add(entry.getValue());
	}
	
	public Stream<Entry<K, Iterator<V>>> stream(){
		return this.entrySet().stream().map(entry -> Utils.kv(entry.getKey(), entry.getValue().iterator()));
	}
	
	public Stream<Entry<K, List<V>>> streamEntries(){
		return this.entrySet().stream();
	}
}

package org.apache.dstream.assembly;

public interface ShuffleWriter<K,V> {

	public void write(K key, V value);
}

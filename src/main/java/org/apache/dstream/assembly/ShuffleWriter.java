package org.apache.dstream.assembly;

import java.util.Map.Entry;

public interface ShuffleWriter<K,V> {

	public void write(Entry<K, V> pair);
}

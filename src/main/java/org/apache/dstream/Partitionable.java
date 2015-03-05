package org.apache.dstream;

import java.util.Map.Entry;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;

public interface Partitionable<K,V> {

	public Triggerable<Entry<K,V>> partition(int partitionSize);
	
	public Triggerable<Entry<K,V>> partition(Partitioner<Entry<K,V>> partitioner);
	
	public Triggerable<Entry<K,V>> partition(SerializableFunction<Entry<K,V>, Integer> partitionerFunction);
}

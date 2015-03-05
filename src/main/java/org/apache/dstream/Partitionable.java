package org.apache.dstream;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;

public interface Partitionable<T> {

	public Persistable<T> partition(int partitionSize);
	
	public Persistable<T> partition(Partitioner<T> partitioner);
	
	public Persistable<T> partition(SerializableFunction<T, Integer> partitionerFunction);
}

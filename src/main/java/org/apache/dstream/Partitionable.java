package org.apache.dstream;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;

public interface Partitionable<T> {

	public Triggerable<T> partition(int partitionSize);
	
	public Triggerable<T> partition(Partitioner<T> partitioner);
	
	public Triggerable<T> partition(SerializableFunction<T, Integer> partitionerFunction);
}

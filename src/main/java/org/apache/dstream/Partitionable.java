package org.apache.dstream;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;

public interface Partitionable<T> {

	Triggerable<T> partition(int partitionSize);
	
	Triggerable<T> partition(Partitioner<T> partitioner);
	
	Triggerable<T> partition(SerializableFunction<T, Integer> partitionerFunction);
}

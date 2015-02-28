package org.apache.dstream;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;

public interface Partitionable<T> {

	public Submittable<T> partition(int partitionSize);
	
	public Submittable<T> partition(Partitioner<T> partitioner);
	
	public Submittable<T> partition(SerializableFunction<T, Integer> partitionerFunction);
}

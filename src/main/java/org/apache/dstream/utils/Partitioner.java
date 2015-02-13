package org.apache.dstream.utils;

public interface Partitioner {

	<T> int getPartition(T input, int reduceTasks);
}

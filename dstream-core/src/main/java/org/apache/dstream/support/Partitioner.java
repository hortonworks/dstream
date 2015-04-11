package org.apache.dstream.support;

import java.io.Serializable;

public interface Partitioner<T> extends Serializable {

	int getPartition(T value);
}

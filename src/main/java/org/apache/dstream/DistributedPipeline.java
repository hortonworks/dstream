package org.apache.dstream;

import java.io.Closeable;

public interface DistributedPipeline<T> extends Computable<T>, Partitionable<T>, Streamable<T>, Closeable {

	public Source<T> getSource();
}

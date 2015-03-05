package org.apache.dstream;

import java.io.Closeable;

import org.apache.dstream.utils.NullType;

public interface DistributedPipeline<T> extends StageEntryPoint<T>, Partitionable<NullType,T>, Streamable<T>, Closeable {

	public Source<T> getSource();
}

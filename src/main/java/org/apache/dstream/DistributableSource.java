package org.apache.dstream;

import java.io.Closeable;




public interface DistributableSource<T> extends StageEntryPoint<T>, Partitionable<T>, Streamable<T>, Closeable {

//	public abstract ComputableSource<T> asJob(String name);
	
}

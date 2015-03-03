package org.apache.dstream;

import java.util.stream.Stream;

public interface Source<T> {

	public default DistributedPipeline<T> asPipeline(String name){
		DefaultDistributedPipeline<T> distributableSource = new DefaultDistributedPipeline<>(this, name);
		return distributableSource;
	}
	
	public abstract Stream<T> toStream();
}

package org.apache.dstream;

import java.util.stream.Stream;

public interface Source<T> {

	public default Pipeline<T> asPipeline(String name){
		DefaultDataPipeline<T> distributableSource = new DefaultDataPipeline<>(this, name);
		return distributableSource;
	}
	
	public abstract Stream<T> toStream();
}

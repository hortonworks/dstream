package org.apache.dstream;

import java.util.stream.Stream;

public interface Source<T> {

	public abstract DistributableSource<T> forJob(String name);
	
	public abstract Stream<T> toStream();
}

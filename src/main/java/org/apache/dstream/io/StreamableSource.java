package org.apache.dstream.io;

import java.util.stream.Stream;

public interface StreamableSource<T> {
	
	public abstract Stream<T> toStream();
	
}

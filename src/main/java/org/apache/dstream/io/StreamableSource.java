package org.apache.dstream.io;

import java.net.URI;
import java.util.stream.Stream;

public interface StreamableSource<T> {
	
	public abstract Stream<T> toStream();
	
	public abstract URI getUri();
}

package org.apache.dstream;

import java.util.stream.Stream;

public interface Streamable<T> {

	public Stream<T> toStream();
}

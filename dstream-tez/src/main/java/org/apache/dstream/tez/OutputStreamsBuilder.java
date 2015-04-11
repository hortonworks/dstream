package org.apache.dstream.tez;

import java.util.stream.Stream;

public interface OutputStreamsBuilder<T> {

	Stream<T>[] build();
	
}

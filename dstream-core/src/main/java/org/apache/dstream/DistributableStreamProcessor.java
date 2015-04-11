package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.BinaryOperator;
import org.apache.dstream.SerializableHelpers.Function;

public abstract class DistributableStreamProcessor {
	
	protected final Function<Stream<?>, Stream<?>> streamProcessingFunction;
	
//	protected final BinaryOperator<?> combiner;
	
	public DistributableStreamProcessor(Function<Stream<?>, Stream<?>> streamProcessingFunction) {
		this.streamProcessingFunction = streamProcessingFunction;
	}

	public abstract Stream<?> process(Stream<?> stream);
}

package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;

/**
 * Base implementation of specialized {@link Function} to process localized {@link Stream}s
 *
 * @param <I>
 * @param <O>
 */
public abstract class AbstractStreamProcessingFunction<I,O> implements Function<Stream<I>, Stream<O>> {
	private static final long serialVersionUID = 1446130673821195933L;
	
	protected final Function<Stream<I>, Stream<O>> streamProcessingFunction;

	/**
	 * 
	 * @param streamProcessingFunction
	 */
	public AbstractStreamProcessingFunction(Function<Stream<I>, Stream<O>> streamProcessingFunction) {
		Assert.notNull(streamProcessingFunction, "'streamProcessingFunction' must not be null");
		this.streamProcessingFunction = streamProcessingFunction;
	}
	
	/**
	 * 
	 */
	@Override
	public Stream<O> apply(Stream<I> streamIn){	
		return this.streamProcessingFunction.apply(streamIn);
	}
}

package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;

/**
 * And implementation of {@link Function} which represents all invocations on 
 * the {@link DistributableStream} within a single stage.
 * 
 * See {@link DistributablePipelineSpecificationBuilder} and {@link DistributableStreamToStreamAdapterFunction}
 *
 */
class ComposableStreamFunction implements Function<Stream<?>, Stream<?>> {
	private static final long serialVersionUID = -1496510916191600010L;
	
	private Function<Stream<?>, Stream<?>> finalFunction;
	
	/**
	 * 
	 */
	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		return this.finalFunction.apply(streamIn);
	}
	
	void add(Function<Stream<?>, Stream<?>> function) {
		this.finalFunction = this.finalFunction == null ? function : function.compose(this.finalFunction);
	}
}

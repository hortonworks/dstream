package org.apache.dstream;

import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;

/**
 * And implementation of {@link Function} which represents all invocations on 
 * the {@link DistributableStream} within a single stage.
 * 
 * See {@link ADSTBuilder} and {@link DistributableStreamToStreamAdapterFunction}
 *
 */
class ComposableStreamFunction implements Function<Stream<?>, Stream<?>> {
	private static final long serialVersionUID = -1496510916191600010L;
	
	private final List<Function<Stream<?>, Stream<?>>> streamOps;
	
	ComposableStreamFunction(List<Function<Stream<?>, Stream<?>>> streamOps){
		this.streamOps = streamOps;
	}

	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		Function<Stream<?>, Stream<?>> composedFunction = this.streamOps.stream().reduce((fa, fb) -> fb.compose(fa)).get(); 
		return composedFunction.apply(streamIn);
	}
}

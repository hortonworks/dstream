package org.apache.dstream;

import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;

class ComposableStreamFunction implements Function<Stream<?>, Stream<?>> {
	private static final long serialVersionUID = -1496510916191600010L;
	
	private final List<DistributableStreamToStreamAdapterFunction> streamOps;
	
	ComposableStreamFunction(List<DistributableStreamToStreamAdapterFunction> streamOps){
		this.streamOps = streamOps;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		Function<Stream, Stream> finalFunction = null;
		
		for (DistributableStreamToStreamAdapterFunction streamFunction : this.streamOps) {
			if (finalFunction == null){
				finalFunction = streamFunction;
			}
			else {
				finalFunction = (Function) finalFunction.andThen(streamFunction);
			}
		}		
		return finalFunction.apply(streamIn);
	}
}

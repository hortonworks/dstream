package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SerializableFunctionConverters.Predicate;

/**
 * An implementation of {@link Function} which will translate Stream-like
 * invocations on {@link DistributableStream} to {@link Stream} operations.
 * It will be created and collected by the ADSTBuilder for each operation 
 * on {@link DistributableStream} (see ADSTBuilder$ComposableStreamFunctionBuilder).
 * Then ADSTBuilder$ComposableStreamFunctionBuilder will construct a single ComposableStreamFunction
 * representing all invocations on {@link DistributableStream}
 */
class DistributableStreamToStreamAdapterFunction implements Function<Stream<?>, Stream<?>>{

	private static final long serialVersionUID = 6836233233261184905L;
	
	private final String streamOperationName;
	
	private final Object sourceFunction;
	
	/**
	 * 
	 * @param streamOperationName
	 * @param sourceFunction
	 */
	DistributableStreamToStreamAdapterFunction(String streamOperationName, Object sourceFunction){
		this.sourceFunction = sourceFunction;
		this.streamOperationName = streamOperationName;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		if (this.streamOperationName.equals("flatMap")){
			return streamIn.flatMap((Function)this.sourceFunction);
		}
		else if (this.streamOperationName.equals("filter")){
			return streamIn.filter((Predicate)this.sourceFunction);
		}
		else if (this.streamOperationName.equals("map")){
			return streamIn.map((Function)this.sourceFunction);
		}
		else {
			throw new UnsupportedOperationException("Operation '" + this.streamOperationName + "' is not supported.");
		}
	}
}

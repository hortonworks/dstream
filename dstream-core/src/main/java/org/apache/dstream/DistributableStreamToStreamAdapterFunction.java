package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.SerializableHelpers.Predicate;

/**
 * An implementation of {@link Function} which will translate Stream-like
 * invocations on {@link DistributableStream} to {@link Stream} operations.
 */
@SuppressWarnings("rawtypes")
class DistributableStreamToStreamAdapterFunction implements Function<Stream, Stream>{

	private static final long serialVersionUID = 6836233233261184905L;
	
	private final String streamOperationName;
	
	private final Object sourceFunction;
	
	DistributableStreamToStreamAdapterFunction(String streamOperationName, Object sourceFunction){
		this.sourceFunction = sourceFunction;
		this.streamOperationName = streamOperationName;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream apply(Stream streamIn) {
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

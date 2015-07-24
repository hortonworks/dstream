package org.apache.dstream.function;

import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Assert;

/**
 * An implementation of {@link Function} which will translate Stream-like
 * invocations on the {@link DistributableStream} to {@link Stream} operations.
 * It will be created and collected by the {@link ExecutionSpecBuilder} for each operation 
 * on the {@link DistributableStream}.
 */
public class DStreamToStreamAdapterFunction implements Function<Stream<?>, Stream<?>>{

	private static final long serialVersionUID = 6836233233261184905L;
	
	private final String streamOperationName;
	
	private final Object sourceFunction;
	
	/**
	 * 
	 * @param streamOperationName
	 * @param sourceFunction
	 */
	public DStreamToStreamAdapterFunction(String streamOperationName, Object sourceFunction){
		Assert.notEmpty(streamOperationName, "'streamOperationName' must not be null or empty");
		if (!(streamOperationName.equals("flatMap") ||
			streamOperationName.equals("map") ||
			streamOperationName.equals("filter"))){
			throw new UnsupportedOperationException("Operation '" + streamOperationName + "' is not supported.");
		}
		Assert.notNull(sourceFunction, "'sourceFunction' must not be null");
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
			// should never happen due to the constructor assertions. Simply concludes IF statement
			throw new UnsupportedOperationException("Operation '" + this.streamOperationName + "' is not supported.");
		}
	}
}

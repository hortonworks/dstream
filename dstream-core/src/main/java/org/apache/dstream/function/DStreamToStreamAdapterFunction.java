package org.apache.dstream.function;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.ReflectionUtils;

/**
 * An implementation of {@link Function} which will translate Stream-like
 * invocations on the {@link DistributableStream} to {@link Stream} operations.
 * It will be created and collected by the {@link ExecutionSpecBuilder} for each operation 
 * on the {@link DistributableStream}.
 */
public class DStreamToStreamAdapterFunction implements Function<Stream<?>, Stream<?>>{
	private static final long serialVersionUID = 6836233233261184905L;
	
	private static final Map<String, Method> supportedOperations = buildSupportedOperations(Stream.of("flatMap", "filter", "map"));
	
	private final String streamOperationName;
	
	private final Object streamOperation;
	
	/**
	 * 
	 * @param streamOperationName
	 * @param sourceFunction
	 */
	public DStreamToStreamAdapterFunction(String streamOperationName, Object streamOperation){
		Assert.notEmpty(streamOperationName, "'streamOperationName' must not be null or empty");
		Assert.notNull(streamOperation, "'streamOperation' must not be null");

		if (!supportedOperations.containsKey(streamOperationName)){
			throw new IllegalArgumentException("Operation 'streamOperationName' is not supported");
		}
		
		this.streamOperation = streamOperation;
		this.streamOperationName = streamOperationName;
	}

	/**
	 * 
	 */
	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		try {
			Method m = supportedOperations.get(this.streamOperationName);
			return (Stream<?>) m.invoke(streamIn, this.streamOperation);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Operation '" + this.streamOperationName + "' is not supported.", e);
		}
	}
	
	/**
	 * 
	 * @param operationsStream
	 * @return
	 */
	private static Map<String, Method> buildSupportedOperations(Stream<String> operationsStream){
		return operationsStream
				.collect(Collectors.toMap(name -> name, name -> ReflectionUtils.findSingleMethod(name, Stream.class)));
	}
}

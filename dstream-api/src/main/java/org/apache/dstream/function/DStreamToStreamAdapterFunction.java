package org.apache.dstream.function;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DStream;
import org.apache.dstream.function.SerializableFunctionConverters.SerFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerPredicate;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.ReflectionUtils;

/**
 * An implementation of {@link SerFunction} which will translate Stream-like
 * invocations on the {@link DStream} to {@link Stream} operations.
 * It is created for each invocation of operations that match operations defined on {@link Stream}.
 * At the time of writing this javadoc these operations are - <i>map, flatMap and filter.</i>
 */
public class DStreamToStreamAdapterFunction implements SerFunction<Stream<?>, Stream<?>>{
	private static final long serialVersionUID = 6836233233261184905L;
	
	private static final Map<String, Method> supportedOperations = buildSupportedOperations(Stream.of("flatMap", "filter", "map"));
	
	private final String streamOperationName;
	
	private final Object streamOperation;
	
	/**
	 * Constructs this function.
	 * 
	 * @param streamOperationName the name of the target operation.
	 * @param streamOperation target operation. The reason why its is Object is because it could be 
	 * 		{@link SerFunction} or {@link SerPredicate}.
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
	 */
	private static Map<String, Method> buildSupportedOperations(Stream<String> operationsStream){
		return operationsStream
				.collect(Collectors.toMap(name -> name, name -> ReflectionUtils.findSingleMethod(name, Stream.class)));
	}
}

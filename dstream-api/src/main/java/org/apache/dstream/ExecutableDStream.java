package org.apache.dstream;

import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Base strategy for defining execution strategies that can support Stream-like 
 * sequential and parallel aggregate operation in the distributable environment. 
 * 
 * @param <T> the type of streamable elements of this {@link ExecutableDStream}
 */
public interface ExecutableDStream<T>{
	
	/**
	 * Will execute the task represented by this {@link ExecutableDStream} returning the result 
	 * as {@link Future} of {@link Stream} which represents the result of the entire execution.
	 * Such result itself consists of {@link Stream}s which represent the following:<br><br>
	 * 
	 * If this {@link ExecutableDStream} is represented by the {@link DStream}, 
	 * then each {@link Stream} contained within the common result corresponds to individual 
	 * result partition: <br>
	 * <pre>
	 * 	Stream - (handle to the entire result of the execution)
	 * 	   |___ Stream - (partition-1)
	 *	   |___ Stream - (partition-2)
	 *	   |___ Stream - (partition-3)
	 * 	</pre>	
	 * T - represents the result type<br>
	 * <br>
	 * This is an <i>terminal</i> operation.
	 * 
	 * @param executionName the name of this execution
	 * @return result as java {@link Future} consisting of {@link Stream}s representing each 
	 * result partition.
	 */
	Future<Stream<Stream<T>>> executeAs(String executionName);
	
	/**
	 * Returns the value to be used to identify the source of this stream.
	 * 
	 * @return value to be used to identify the source of this stream.
	 */
	String getSourceIdentifier();
}

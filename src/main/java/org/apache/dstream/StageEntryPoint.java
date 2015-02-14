package org.apache.dstream;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.dstream.StreamExecutionContext.IntermediateKVResult;
import org.apache.dstream.StreamExecutionContext.SerializableFunction;

/**
 * Strategy which defines computation methods that serve as entry points for distributed computation stages 
 * 
 * @param <T>
 */
public interface StageEntryPoint<T> {
	/**
	 * Defines <b>intermediate</b> computation entry point (starting point for a new Stage/Vertex in a 
	 * DAG-like implementation) for a {@link Stream} which produces KEY/VALUE pairs. Result of 
	 * intermediate computation could be further reduced and/or partitioned via {@link IntermediateKVResult} 
	 * and forwarded to the next computation via {@link IntermediateStageEntryPoint}.
	 * 
	 * <blockquote>
     * <pre>
     * StreamExecutionContext.of(. . .)
     * 		.computeAsKeyValue(String.class, Integer.class, stream -> stream
	 *			.flatMap(s -> Stream.of(s.split("\\s+")))
	 *			.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	 *		).reduceByKey((a,b) -> a + b, 2)
	 *		.computeAsKeyValue(. . .)
     * </pre>
     * </blockquote>
     * 
     * See {@link #compute(SerializableFunction)} for <b>terminal</b> computation entry point.
	 * 
	 * @param outputKey - the type of KEY
	 * @param outputVal - the type of VALUE
	 * @param function  - {@link Stream} lambda which must return {@link Map} with KEY/VALUE of types
	 * 					  identified by 'outputKey'/'outputVal'
	 * @return
	 */
	public abstract <K,V,R> IntermediateKVResult<K,V> computeAsKeyValue(Class<K> outputKey, Class<V> outputVal, SerializableFunction<Stream<T>, Map<K,V>> function);
	
	/**
	 * Defines <b>terminal</b> computation entry point (starting point for a new Stage/Vertex in a 
	 * DAG-like implementation) for a {@link Stream}. Result of terminal computation will be returned as is.
	 * 
	 * <blockquote>
     * <pre>
     * long count = StreamExecutionContext.of(. . .)
     * 		.compute(stream -> stream
	 *			.flatMap(s -> Stream.of(s.split("\\s+")))
	 *			.count()
	 *		)
	 * or
	 * 
	 * Map<String, Integer> resultMap = StreamExecutionContext.of(. . .)
	 * 		.compute(stream -> stream
	 *			.flatMap(s -> Stream.of(s.split("\\s+")))
	 *			.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	 *		)
     * </pre>
     * </blockquote>
     * 
     * See {@link #computeAsKeyValue(Class, Class, SerializableFunction)} for <b>intermediate</b> computation entry point.
	 * 
	 * @param outputKey - the type of KEY
	 * @param outputVal - the type of VALUE
	 * @param function  - {@link Stream} lambda which must return {@link Map} with KEY/VALUE of types
	 * 					  identified by 'outputKey'/'outputVal'
	 * @return
	 */
	public abstract <R> R compute(SerializableFunction<Stream<T>, R> function);
}

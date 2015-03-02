package org.apache.dstream;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.dstream.utils.NullType;
import org.apache.dstream.utils.SerializableFunction;

/**
 * Strategy which defines computation methods that serve as initial entry points for distributed computation stages.
 * 
 * @param <T>
 */
public interface StageEntryPoint<T> {
//	/**
//	 * Defines <b>intermediate</b> computation entry point (starting point for a new Stage/Vertex in a 
//	 * DAG-like implementation) for a {@link Stream} which produces KEY/VALUE pairs. Result of 
//	 * intermediate computation could be further reduced and/or partitioned via {@link Merger} 
//	 * and forwarded to the next computation via {@link Submittable}.
//	 * 
//	 * <blockquote>
//     * <pre>
//     * StreamExecutionContext.of(. . .)
//     * 		.computeAsKeyValue(String.class, Integer.class, stream -> stream
//	 *			.flatMap(s -> Stream.of(s.split("\\s+")))
//	 *			.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
//	 *		).reduceByKey((a,b) -> a + b, 2)
//	 *		.computeAsKeyValue(. . .)
//     * </pre>
//     * </blockquote>
//     * 
//     * See {@link #compute(SerializableFunction)} for <b>terminal</b> computation entry point.
//	 * 
//	 * @param outputKey - the type of KEY
//	 * @param outputVal - the type of VALUE
//	 * @param function  - {@link Stream} lambda which must return {@link Map} with KEY/VALUE of types
//	 * 					  identified by 'outputKey'/'outputVal'
//	 * @return
//	 */
	
	//private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public int computeInt(SerializableFunction<Stream<T>, Integer> function);
	
	public long computeLong(SerializableFunction<Stream<T>, Long> function);
	
	public double computeDouble(SerializableFunction<Stream<T>, Double> function);
	
	public boolean computeBoolean(SerializableFunction<Stream<T>, Boolean> function);

	public <R> IntermediateResult<NullType, R> computeCollection(SerializableFunction<Stream<T>, Collection<R>> function);
	
	public <V> IntermediateResult<T,V> computePairs(SerializableFunction<Stream<T>, Map<T,V>> function);
}

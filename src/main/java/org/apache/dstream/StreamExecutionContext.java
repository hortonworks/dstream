package org.apache.dstream;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.utils.Partitioner;
/**
 * Base class which defines <b>Execution Context</b> for executing 
 * Java {@link Stream}s.
 *
 * @param <T>
 */
public abstract class StreamExecutionContext<T> {
	
	/**
	 * Factory method that will return implementation of this {@link StreamExecutionContext}
	 */
	public static <T> StreamExecutionContext<T> of(Streamable<T> source) {
		return null;
	}
	
	/**
	 * Defines <b>intermediate</b> computation entry point for a {@link Stream} which produces KEY/VALUE pairs.
	 * Result of intermediate computation could be further reduced and/or partitioned via {@link IntermediateKVResult} 
	 * and forwarded to the next computation via {@link IntermediateEntryPoint}.
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
	 * Defines <b>terminal</b> computation entry point for a {@link Stream}.
	 * Result of terminal computation will be returned as is.
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
	
	/**
	 * Returns the source of this stream as {@link Streamable}
	 * 
	 * @return
	 */
	public abstract Streamable<T> getSource();
	
	/**
	 * Returns the raw {@link InputStream} to the result data set.
	 * @return
	 */
	//http://stackoverflow.com/questions/22919013/inputstream-to-hadoop-sequencefile
	public abstract InputStream toInputStream(); 
	
	/**
	 * Returns the {@link Stream} to the result data set allowing result data to be streamed for 
	 * local processing (e.g., iterate over results)
	 * 
	 * @return
	 */
	public abstract Stream<T> stream();
	
	/**
	 * Simple {@link Serializable} wrapper over {@link Function}
	 * 
	 * @param <T>
	 * @param <R>
	 */
	public interface SerializableFunction<T,R> extends Function<T, R>, Serializable {}
	
	/**
	 * Strategy which represents intermediate KEY/VALUE results as {@link Entry}. It is returned by 
	 * {@link StreamExecutionContext#computeAsKeyValue(Class, Class, SerializableFunction)} method.
	 * <br>
	 * Intermediate results are the post-shuffle (read from the shuffled input) 
	 * 
	 * @param <K>
	 * @param <V>
	 */
	public interface IntermediateKVResult<K,V> {
		/**
		 * Will perform a post-shuffle reduce by key, producing the same Key/Value types as declared by 
		 * {@link IntermediateKVResult#computeAsKeyValue(Class, Class, SerializableFunction)} method.
		 * 
		 * @param mergeFunction
		 * @param reducers
		 * @return
		 */
		public IntermediateEntryPoint<Entry<K,V>> reduceByKey(BinaryOperator<V> mergeFunction, int reducers);
			
		/**
		 * Will perform a post-shuffle reduce by value, producing the same Key/Value types as declared by 
		 * {@link IntermediateKVResult#computeAsKeyValue(Class, Class, SerializableFunction)} method.
		 * 
		 * @param mergeFunction
		 * @param reducers
		 * @return
		 */
		public IntermediateEntryPoint<Entry<K,V>> reduceByValue(BinaryOperator<K> mergeFunction, int reducers);
		
		/**
		 * Will perform a post-shuffle reduce passing the whole {@link Entry}, producing the same Key/Value types as declared by 
		 * {@link IntermediateKVResult#computeAsKeyValue(Class, Class, SerializableFunction)} method.
		 * 
		 * See {@link #reduceByKey(BinaryOperator, int)} and {@link #reduceByValue(BinaryOperator, int)} as well
		 * 
		 * @param mergeFunction
		 * @param reducers
		 * @return
		 */
		public IntermediateEntryPoint<Entry<K,V>> reduce(BinaryOperator<Entry<K,V>> mergeFunction, int reducers);
		
		/**
		 * Will partition the intermediate result using provided {@link Partitioner}
		 * 
		 * @param partitioner
		 * @return
		 */
		public IntermediateEntryPoint<Entry<K,V>> partition(Partitioner partitioner);
		
		/**
		 * ill partition the intermediate result using provided partitioning function.
		 * 
		 * @param partitionerFunction
		 * @return
		 */
		public IntermediateEntryPoint<Entry<K,V>> partition(SerializableFunction<Entry<K,V>, Integer> partitionerFunction);
	}
	
	/**
	 * Strategy for defining an intermediate entry point.
	 *
	 * @param <T>
	 */
	public interface IntermediateEntryPoint<T> {
		/**
		 * Defines a new compute stage on {@link Stream}. 
		 * 
		 * @param outputKey
		 * @param outputVal
		 * @param function
		 * @return
		 */
		public <K,V,R> IntermediateKVResult<K,V> computeAsKeyValue(Class<K> outputKey, Class<V> outputVal, SerializableFunction<Stream<T>, Map<K,V>> function);
		
		/**
		 * Will save the results of the intermediate computation to the disk based on provided {@link OutputSpecification}
		 * returning new {@link StreamExecutionContext}
		 * 
		 * @param outputSpec
		 * @return
		 */
		public StreamExecutionContext<T> saveAs(OutputSpecification outputSpec);
	}
	
}

package org.apache.dstream;

import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.dstream.support.DefaultHashPartitioner;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.SerializableFunctionConverters.BiFunction;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;

public interface DistributablePipeline<T> extends Distributable<T> {

	/**
	 * Factory method which returns a sequential {@code DistributablePipeline} of 
	 * elements of the provided type. The source for the {@link DistributablePipeline} 
	 * could be provided at the configuration time by setting {@value #SRC_SUPPLIER} 
	 * or {@value #SRC_URL_SUPPLIER}. You can also provide the source by using 
	 * {@link #ofType(Class, Supplier)} factory method. 
	 * 
	 * 
	 * @param sourceItemType
	 * @return
	 */
	public static <T> DistributablePipeline<T> ofType(Class<T> sourceItemType) {	
		return ofType(sourceItemType, null);
	}
	
	/**
	 * Factory method which returns a sequential {@code DistributablePipeline} of 
	 * elements of the provided type and source of the stream supplied by 
	 * the {@link Supplier}
	 * 
	 * Custom suppliers could be provided allowing program arguments to be used in
	 * predicate logic to determine sources dynamically.
	 * 
	 * @param sourceItemType
	 * @param sourceSuppliers
	 * @return the new {@link DistributablePipeline} of type T
	 */
	@SuppressWarnings("unchecked")
	public static <T> DistributablePipeline<T> ofType(Class<T> sourceItemType, SourceSupplier<?> sourcesSupplier) {	
		return ADSTBuilder.getAs(sourceItemType, sourcesSupplier, DistributablePipeline.class);
	}
	
	/**
	 * Returns a pipeline consisting of the results of applying computation to the 
	 * elements of the underlying stream.
	 * 
	 * @param computeFunction a mapping function to map {@link Stream&lt;T&gt;} to {@link Stream&lt;R&gt;}.
	 * @return the new {@link DistributablePipeline} of type R
	 */
	<R> DistributablePipeline<R> compute(Function<? extends Stream<T>, ? extends Stream<R>> computeFunction);
	
	
//	/**
//	 * Returns a pipeline consisting of the key/value results of applying computation to the 
//	 * elements of the underlying stream.
//	 * 
//	 * @param computeFunction a mapping function to map {@link Stream&lt;T&gt;} to key/value {@link Stream&lt;Entry&lt;K,V&gt;&gt;}.
//	 * @return the new {@link DistributableKeyValuePipeline} of type K,V
//	 */
//	<K,V> DistributableKeyValuePipeline<K,V> computeKeyValues(Function<? extends Stream<T>, ? extends Stream<Entry<K,V>>> computeFunction);
	
	
	/**
	 * Returns a pipeline consisting of the results of applying reduce functionality on
	 * the elements of the underlying stream.
	 * 
	 * This is an intermediate operation
	 * 
	 * @param classifier the classifier function mapping input elements to keys
	 * @param valueMapper a mapping function to produce values
	 * @param valueMerger a merge function, used to resolve collisions between
     *                      values associated with the same key
	 * @return the new {@link DistributablePipeline} of type {@link Entry&lt;K,V&gt;}
	 */
	<K,V> DistributablePipeline<Entry<K, V>> reduce(Function<? super T, ? extends K> classifier, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> valueMerger);
	
	/**
	 * 
	 * @param classifier the classifier function mapping input elements to keys
	 * @return
	 */
	<K,V> DistributablePipeline<Entry<K, V[]>> group(Function<? super T, ? extends K> classifier);

	/**
	 * Will calculate partitions using the entire value of each element of the stream.
	 * 
	 * Unless configured via {@link Distributable#PARTITIONER} property, the system will 
	 * use default {@link DefaultHashPartitioner}
	 * 
	 * @return the new {@link DistributablePipeline} of type T
	 */
	DistributablePipeline<T> partition();
	
	/**
	 * Will calculate partitions using the resulting value of applying classifier function on each 
	 * element of the stream.
	 * 
	 * Unless configured via {@link Distributable#PARTITIONER} property, the system will 
	 * use default {@link DefaultHashPartitioner}
	 * 
	 * @return the new {@link DistributablePipeline} of type T
	 */
	<V> DistributablePipeline<T> partition(Function<? super T, ? extends V> classifier);
	
	/**
	 * Will join two {@link DistributablePipeline}s together producing new {@link DistributablePipeline} of type R
	 * 
	 * The 'joinFunction' 
	 * 
	 * @param pipelineR producer of target {@link Stream} this {@link Stream} will be joined with.
	 * @param joinFunction a {@link BiFunction} where the actual join between {@link Stream}s will be performed.
	 * 
	 * Also see {@link DistributableKeyValuePipeline#joinByKey(DistributableKeyValuePipeline)} for key-based join.
	 * 
	 * @return the new {@link DistributablePipeline} of type R
	 */
	<TT,R> DistributablePipeline<R> join(DistributablePipeline<TT> pipelineR, 
			BiFunction<Stream<T>, Stream<TT>, Stream<R>> joinFunction);
}

package org.apache.dstream;

import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Pair;
/**
 * 
 * @param <T> the type of the pipeline elements
 */
public interface DistributablePipeline<T> extends DistributableExecutable<T> {
	/**
	 * Factory method which returns a sequential {@code DistributablePipeline} of 
	 * elements of the provided type and source of the stream supplied by 
	 * the {@link Supplier}
	 * 
	 * Custom suppliers could be provided allowing program arguments to be used in
	 * predicate logic to determine sources dynamically.
	 * 
	 * @param sourceItemType
	 * @param pipelineName
	 * @return the new {@link DistributablePipeline} of type T
	 * 
	 * @param <T> the type of the stream elements
	 */
	@SuppressWarnings("unchecked")
	public static <T> DistributablePipeline<T> ofType(Class<T> sourceItemType, String pipelineName) {	
		return ExecutionContextSpecificationBuilder.getAs(sourceItemType, pipelineName, DistributablePipeline.class);
	}
	
	/**
	 * Returns a pipeline consisting of the results of applying computation to the 
	 * elements of the underlying stream.
	 * 
	 * @param computeFunction a mapping function to map {@link Stream}[T] to {@link Stream}[R].
	 * @return the new {@link DistributablePipeline} of type R
	 * 
	 * @param <R> the type of the elements of the new pipeline
	 */
	<R> DistributablePipeline<R> compute(Function<? extends Stream<T>, ? extends Stream<R>> computeFunction);
	
	/**
	 * Will reduce all values for a given key to a single value using provided 
	 * {@link BinaryOperator} 
	 * 
	 * This is an intermediate operation
	 * 
	 * @param keyClassifier the classifier function mapping input elements to keys
	 * @param valueMapper a mapping function to produce values
	 * @param reducer a merge function, used to resolve collisions between
     *                      values associated with the same key
	 * @return the new {@link DistributablePipeline} of type {@link Entry}[K,V]
	 * 
	 * @param <K> key type
	 * @param <V> value type
	 */
	<K,V> DistributablePipeline<Entry<K, V>> reduce(Function<? super T, ? extends K> keyClassifier, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> reducer);
	
//	/**
//	 * 
//	 * @param classifier the classifier function mapping input elements to keys
//	 * @return
//	 * 
//	 * @param <K> key type
//	 * @param <V> value type
//	 */
//	<K,V> DistributablePipeline<Entry<K, V[]>> group(Function<? super T, ? extends K> classifier);

//	/**
//	 * Will calculate partitions using the entire value of each element of the stream.
//	 * 
//	 * 
//	 * @return the new {@link DistributablePipeline} of type T
//	 */
//	DistributablePipeline<T> partition();
//	
//	/**
//	 * Will calculate partitions using the resulting value of applying classifier function on each 
//	 * element of the stream.
//	 * 
//	 * @return the new {@link DistributablePipeline} of type T
//	 * 
//	 * @param <V>
//	 */
//	<V> DistributablePipeline<T> partition(Function<? super T, ? extends V> classifier);
	
	/**
	 * Join based on common predicate
	 * 
	 * @param lKeyMapper
	 * @param lValueMapper
	 * @param pipelineR
	 * @param rKeyMapper
	 * @param rValueMapper
	 * @return
	 */
	<TT, K, VL, VR> DistributablePipeline<Entry<K, Pair<VL,VR>>> join(DistributablePipeline<TT> pipelineP,
																	  Function<? super T, ? extends K> hashKeyClassifier,
																	  Function<? super T, ? extends VL> hashValueMapper,
																	  Function<? super TT, ? extends K> probeKeyClassifier,
																	  Function<? super TT, ? extends VR> probeValueMapper);
	
}

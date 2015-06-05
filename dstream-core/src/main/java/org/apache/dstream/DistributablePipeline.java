package org.apache.dstream;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Pair;
/**
 * Pipeline-style specialization strategy of {@link DistributableExecutable} which 
 * defines data operations to provide Functions that operate on standard java {@link Stream}.<br>
 * Also see {@link DistributableStream}.<br>
 * Below is the example of rudimentary <i>Word Count</i> written in this style:<br>
 * <pre>
 * DistributablePipeline.ofType(String.class, "wc")
 *   .compute(stream -> stream
 *      .flatMap(line -> Stream.of(line.split("\\s+")))
 *      .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream())
 *   .reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
 *   .executeAs("WordCount"); 
 * </pre>
 *  
 * @param <T> the type of the pipeline elements
 */
public interface DistributablePipeline<T> extends DistributableExecutable<T> {
	
	/**
	 * Factory method which creates an instance of the {@code DistributablePipeline} of type T.
	 * 
	 * @param sourceItemType the type of the elements of this pipeline
	 * @param pipelineName the name of this pipeline
	 * @return the new {@link DistributablePipeline} of type T
	 * 
	 * @param <T> the type of pipeline elements
	 */
	@SuppressWarnings("unchecked")
	public static <T> DistributablePipeline<T> ofType(Class<T> sourceItemType, String pipelineName) {	
		return ExecutionSpecBuilder.getAs(sourceItemType, pipelineName, DistributablePipeline.class);
	}
	
	/**
	 * Operation to provide a computation {@link Function} to be applied on each input partition of the 
	 * distributable data set.<br>
	 * Each partition as well as result are both represented as {@link Stream} - 
	 * {@link Function&lt;Stream, Stream&gt;}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * 
	 * @param computeFunction a mapping function to map {@link Stream}&lt;T&gt; to {@link Stream}&lt;R&gt;.
	 * @return {@link DistributablePipeline} of type R
	 * 
	 * @param <R> the type of the elements of the new pipeline
	 */
	<R> DistributablePipeline<R> compute(Function<? extends Stream<T>, ? extends Stream<R>> computeFunction);
	
	/**
	 * Operation to provide a set of functions to group and reduce data across distributable 
	 * data set into Key/Value pairs based on the common <i>classifier</i> (e.g., key).<br>
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * 
	 * @param classifier function to extract classifier
	 * @param valueMapper function to extract values
	 * @param reducer a merge function, to resolve collisions between
     *                      values associated with the same key
	 * @return {@link DistributablePipeline} of type {@link Entry}&lt;K,V&gt;
	 * 
	 * @param <K> classifier type (key)
	 * @param <V> value type
	 */
	<K,V> DistributablePipeline<Entry<K, V>> reduce(Function<? super T, ? extends K> classifier, 
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
	 * Operation to provide a set of functions to join data set represented by this {@link DistributablePipeline} 
	 * with another {@link DistributablePipeline} based on the common predicate (hash join).<br>
	 * 
	 * @param pipelineP instance of {@link DistributablePipeline} to join with - (probe)
	 * @param hashKeyClassifier function to extract Key from this instance of the {@link DistributablePipeline} - (hash)
	 * @param hashValueMapper function to extract value from this instance of the {@link DistributablePipeline} - (hash)
	 * @param probeKeyClassifier function to extract Key from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @param probeValueMapper function to extract value from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @return {@link DistributablePipeline} of type {@link Entry}&lt;K, {@link Pair}&lt;VL,VR&gt;&gt;
	 * 
	 * @param <TT> the type of elements of the {@link DistributablePipeline} to join with - (probe)
	 * @param <K>  the type of common classifier (key)
	 * @param <VH> the type of values of the elements extracted from this instance of the {@link DistributablePipeline} - hash
	 * @param <VP> the type of values of the elements extracted from the joined instance of the {@link DistributablePipeline} - probe
	 */
	<TT, K, VL, VR> DistributablePipeline<Entry<K, Pair<VL,VR>>> join(DistributablePipeline<TT> pipelineP,
																	  Function<? super T, ? extends K> hashKeyClassifier,
																	  Function<? super T, ? extends VL> hashValueMapper,
																	  Function<? super TT, ? extends K> probeKeyClassifier,
																	  Function<? super TT, ? extends VR> probeValueMapper);
}

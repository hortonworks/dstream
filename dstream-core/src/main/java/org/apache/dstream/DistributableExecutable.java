package org.apache.dstream;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.dstream.support.SourceFilter;

/**
 * Base strategy for defining execution strategies that can support Stream-like 
 * sequential and parallel aggregate operation in the distributable environment. 
 * 
 * @param <T> the type of elements of the task represented by this {@link DistributableExecutable}
 */
public interface DistributableExecutable<T> {
	
	/**
	 * Will execute the task represented by this {@link DistributableExecutable} returning the result 
	 * as {@link Future} of {@link Stream} of partitions where each partition itself is represented 
	 * as {@link Stream}.
	 * 
	 * How the actual output will be stored or how long will it remain after the resulting 
	 * {@link Stream} is closed is undefined and is implementation dependent.
	 * 
	 * @param executionName the name of the task represented by this {@link DistributableExecutable}
	 * @return {@link Future} of {@link Stream} of partitions where each partition represented as {@link Stream}
	 */
	Future<Stream<Stream<T>>> executeAs(String executionName);
	
	Future<Stream<Stream<T>>> executeAs(String executionName, SourceFilter<?> sourceFilter);
	
//	/**
//	 * Will execute the task represented by this {@link DistributableExecutable} returning the result 
//	 * as {@link Future} of {@link Stream} of partitions where each partition itself is represented 
//	 * as {@link Stream}.
//	 * 
//	 * How the actual output will be stored or how long will it remain after the resulting 
//	 * {@link Stream} is closed is undefined and is implementation dependent.
//	 * 
//	 * @param executionName the name of the task represented by this {@link DistributableExecutable}
//	 * @param sourcesSupplier the supplier of the source of data to be processed
//	 * @return {@link Future} of {@link Stream} of partitions where each partition represented as {@link Stream}
//	 */
//	Future<Stream<Stream<T>>> executeAs(String executionName, SourceSupplier<?> sourcesSupplier);
}

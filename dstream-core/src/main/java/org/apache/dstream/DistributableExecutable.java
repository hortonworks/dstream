package org.apache.dstream;

import java.net.URI;
import java.util.stream.Stream;

/**
 * Base strategy for defining execution strategies that can support Stream-like 
 * sequential and parallel aggregate operation in the distributable environment. 
 * 
 * @param <T> the type of elements of the task represented by this {@link DistributableExecutable}
 */
public interface DistributableExecutable<T> {
	
	/**
	 * Will execute the task represented by this {@link DistributableExecutable} returning the result 
	 * as {@link Stream} of partitions where each partition itself is represented as {@link Stream}.
	 * 
	 * How the actual output will be stored or how long will it remain after the resulting 
	 * {@link Stream} is closed is undefined. Please see {@link #executeAs(String, URI)} which 
	 * allows one to provide output path URI to permanently store results.
	 * 
	 * @param name the name of the task represented by this {@link DistributableExecutable}
	 * @return {@link Stream} of partitions where each partition represented as {@link Stream}
	 */
	Stream<Stream<T>> executeAs(String name);
	
	/**
	 * Will execute the task represented by this {@link DistributableExecutable} returning the result 
	 * as {@link Stream} of partitions where each partition itself is represented as {@link Stream}.
	 * 
	 * Unlike the {@link #executeAs(String)} operation, this operation allows you to specify
	 * output location {@link URI} signaling the intention for the output to be permanently stored
	 * at the provided location.
	 * 
	 * @param name the name of the task represented by this {@link DistributableExecutable}
	 * @return {@link Stream} of partitions where each partition represented as {@link Stream}
	 */
	Stream<Stream<T>> executeAs(String name, URI outputPath);
}

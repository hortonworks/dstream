package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.io.OutputSpecification;

/**
 * Strategy which defines functionality to trigger distributed execution of the {@link Stream}.
 * 
 * @param <R> - the result type
 */
public interface Submittable<R> extends StageEntryPoint<R>{
	/**
	 * Will trigger execution of the source {@link Stream} saving its to the location 
	 * identified by the {@link OutputSpecification} and returning a {@link Stream} over the 
	 * results of this execution, allowing immediate access to the result regardless of its size.
	 * 
	 * Aside from the output location, {@link OutputSpecification} implementation may contain environment specific 
	 * properties (e.g., In Hadoop - InputFormat, Writable etc.) 
	 * 
	 * @param outputSpec
	 * @return
	 */
	public Stream<R> save(OutputSpecification outputSpec);
	
	/**
	 * Will trigger execution of the source {@link Stream} returning a {@link Stream} over the results of the execution, 
	 * thus allowing immediate access to the result regardless of its size.
	 * 
	 * @return
	 */
	public Stream<R> collect();
}
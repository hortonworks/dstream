package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.io.OutputSpecification;

/**
 * Strategy which extends {@link StageEntryPoint} and defines functionality which will result 
 * in submission of job to execution environment.
 * 
 * @param <T>
 */
public interface Submittable<T> extends StageEntryPoint<T>{
	/**
	 * Will save the results of the intermediate computation to the disk based on 
	 * provided {@link OutputSpecification} returning new {@link StreamExecutionContext}
	 * 
	 * @param outputSpec
	 * @return
	 */
	public Stream<T> saveAs(OutputSpecification outputSpec);
}
package org.apache.dstream;

import java.nio.file.FileSystem;

/**
 * Strategy which defines additional triggering operators to specify <i>how</i> 
 * and <i>where</i> to store the results of the computation.
 * 
 * @param <R> - the result type
 */
public interface Persistable<R> extends Computable<R>{
	/**
	 * Will trigger execution of the {@link DataPipeline} saving its results to the location 
	 * identified by the {@link OutputSpecification} and returning a new {@link DataPipeline} over the 
	 * results of this execution.
	 * 
	 * Aside from the output location, {@link OutputSpecification} implementation may contain environment specific 
	 * properties (e.g., In Hadoop - OutputFormat, Writable etc.) 
	 * 
	 * @param outputSpec
	 * @return
	 */
	public DataPipeline<R> save(OutputSpecification outputSpec);
	
	/**
	 * Will trigger execution of the {@link DataPipeline} saving its results to the default location 
	 * on the provided {@link FileSystem}. The default location is <i>jobName + "/out"</i>
	 * It returns a new {@link DataPipeline} over the results of this execution.
	 * 
	 * @param fs
	 * @return
	 */
	public DataPipeline<R> save(FileSystem fs);
}
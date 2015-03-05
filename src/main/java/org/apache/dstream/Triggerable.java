package org.apache.dstream;

import java.nio.file.FileSystem;
import java.util.stream.Stream;

/**
 * Strategy which defines functionality to trigger distributed execution of the {@link Stream}.
 * 
 * The reason why its extends form {@link StageEntryPoint} is to enable implicit triggering and/or
 * provide an ability to assemble a single execution pipeline consisting of multiple stages.
 * TODO - need to refine the above argument 
 * 
 * @param <R> - the result type
 */
public interface Triggerable<R> extends StageEntryPoint<R>{
	/**
	 * Will trigger execution of the {@link DistributedPipeline} saving its results to the location 
	 * identified by the {@link OutputSpecification} and returning a new {@link DistributedPipeline} over the 
	 * results of this execution.
	 * 
	 * Aside from the output location, {@link OutputSpecification} implementation may contain environment specific 
	 * properties (e.g., In Hadoop - OutputFormat, Writable etc.) 
	 * 
	 * @param outputSpec
	 * @return
	 */
	public DistributedPipeline<R> save(OutputSpecification outputSpec);
	
	/**
	 * Will trigger execution of the {@link DistributedPipeline} saving its results to the default location 
	 * on the provided {@link FileSystem}. The default location is <i>jobName + "/out"</i>
	 * It returns a new {@link DistributedPipeline} over the results of this execution.
	 * 
	 * @param fs
	 * @return
	 */
	public DistributedPipeline<R> save(FileSystem fs);
}
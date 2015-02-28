package org.apache.dstream.io;

import java.nio.file.Path;

import org.apache.dstream.IntermediateResult;

/**
 * Base marker interface allowing for the definition of the output specification to be used 
 * within specific execution context.
 * At the moment it is not well defined and and serves primarily to satisfy the type requirements
 * of {@link IntermediateResult#saveAs(OutputSpecification)} method.
 * 
 * Things that can go into further definition and implementation of this interface are:
 * - Output Format
 * - Key/Value types
 * - Output path
 * - etc.
 *
 */
public interface OutputSpecification {

	public Path getOutputPath();
	
	public <T> StreamSource<T> toStreamableSource();
}

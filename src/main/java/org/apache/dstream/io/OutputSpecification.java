package org.apache.dstream.io;

import org.apache.dstream.StreamExecutionContext.IntermediateKVResult;

/**
 * Marker interface allowing for the definition of the output specification to be used 
 * within specific execution context.
 * At the moment it is not well defined and and serves primarily to satisfy the type requirements
 * of {@link IntermediateKVResult#saveAs(OutputSpecification)} method.
 *
 */
public interface OutputSpecification {

}

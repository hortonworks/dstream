package org.apache.dstream;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.dstream.utils.TypeUtils;

/**
 * Base strategy for defining execution strategies that can support Stream-like 
 * sequential and parallel aggregate operation in the distributable environment. 
 * 
 * @param <T> the type of streamable elements of this {@link DistributableExecutable}
 */
public interface DistributableExecutable<T> {
	
	/**
	 * Will execute the task represented by this {@link DistributableExecutable} returning the result 
	 * as {@link Future} of {@link Stream} which represents the result of the entire execution.
	 * Such result itself consists of {@link Stream}s which could represent one of two things:<br><br>
	 * 
	 * 1. If this {@link DistributableExecutable} is represented by the {@link DistributableStream} 
	 * or {@link DistributablePipeline}, then each {@link Stream} contained within the common result 
	 * corresponds to individual result partition: <br>
	 * <pre>
	 * 	Stream - (handle to the entire result of the execution)
	 * 	   |___ Stream - (partition-1)
	 *	   |___ Stream - (partition-2)
	 *	   |___ Stream - (partition-3)
	 * 	</pre>	
	 * T - represents the result type
	 * 
	 * <br><br>
	 * 
	 * 2. If this {@link DistributableExecutable} is represented by the {@link ExecutionGroup}, then 
	 * each {@link Stream} contained within the common result itself represents the entire result of 
	 * the execution of each individual pipeline (e.g., {@link DistributableStream} or 
	 * {@link DistributablePipeline}) which themselves contain {@link Stream}s corresponding to individual 
	 * result partitions:<br>
	 * <pre>
	 * 	Stream - (handle to the entire result of the execution of the group)
	 * 	   |___ Stream - (handle to the entire result of the execution of the pipeline-1)
	 *             |___ Stream - (partition-1)
	 *             |___ Stream - (partition-2)
	 *	   |___ Stream - (handle to the entire result of the execution of the pipeline-2)
	 *             |___ Stream - (partition-1)
	 *	   |___ Stream - (handle to the entire result of the execution of the pipeline-3)
	 *             |___ Stream - (partition-1)
	 *             |___ Stream - (partition-2)
	 *             |___ Stream - (partition-3)
	 * 	</pre>
	 * In this case T is ?, since result types in the group may be different. Casting could be simplified by using {@link TypeUtils}.
	 * <br><br>
	 * 
	 * How the actual output will be stored or how long will it remain after the resulting 
	 * {@link Stream} is closed is undefined and is implementation dependent.
	 * 
	 * @param executionName the name of this execution
	 * @return 
	 */
	Future<Stream<Stream<T>>> executeAs(String executionName);
	
	/**
	 * Returns the execution name of this {@link DistributableExecutable}.
	 * @return
	 */
	String getName();
}

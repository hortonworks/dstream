package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strategy for grouping multiple pipelines into a single execution
 */
public interface ExecutionGroup extends DistributableExecutable<Stream<? extends Object>> {
	
	static final Logger logger = LoggerFactory.getLogger(ExecutionGroup.class);
	
	/**
	 * Will group multiple instances of {@link DistributableExecutable} (<i>execution pipelines</i>)
	 * into a single execution.
	 * Duplicate instances of {@link DistributableExecutable} where <i>DistributableExecutable-A.equals(DistributableExecutable-B)</i> 
	 * will be discarded. 
	 * 
	 * @param executionGroupName the name of the execution
	 * @param distributables and array of pipelines to be grouped. Must have at least one element.
	 * @return and instance of {@link DistributableExecutable} as {@link ExecutionGroup}.
	 */
	public static ExecutionGroup create(String executionGroupName, DistributableExecutable<?>... distributables){
		Assert.notEmpty(executionGroupName, "'executionGroupName' must not be null or empty");
		Assert.notEmpty(distributables, "'distributables' must not be null and must contain at least one element");
		throw new UnsupportedOperationException("ExecutionGroup is temporarily not supported");
		
	}
}

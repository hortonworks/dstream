package org.apache.dstream;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.dstream.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strategy for grouping multiple pipelines into a single execution
 */
public interface JobGroup extends DistributableExecutable<Stream<? extends Object>> {
	
	static final Logger logger = LoggerFactory.getLogger(JobGroup.class);
	
	/**
	 * Will group multiple instances of {@link DistributableExecutable} (<i>execution pipelines</i>)
	 * into a single execution.
	 * Duplicate instances of {@link DistributableExecutable} where <i>DistributableExecutable-A.equals(DistributableExecutable-B)</i> 
	 * will be discarded. 
	 * 
	 * @param jobGroupName the name of the execution
	 * @param distributables and array of pipelines to be grouped. Must have at least one element.
	 * @return
	 */
	public static JobGroup create(String jobGroupName, DistributableExecutable<?>... distributables){
		Assert.notEmpty(jobGroupName, "'jobGroupName' must not be null or empty");
		Assert.notEmpty(distributables, "'distributables' must not be null and must contain at least one element");
		
		Set<DistributableExecutable<?>> s = new LinkedHashSet<DistributableExecutable<?>>();
		for (DistributableExecutable<?> distributableExecutable : distributables) {
			if (s.contains(distributableExecutable)){
				logger.warn("DistributableExecutable: " + distributableExecutable + 
						" already exist in the group. Subsequent istance is excluded from the group");
			}
			else {
				s.add(distributableExecutable);
			}
		}
		return ExecutionContextSpecificationBuilder.asGroupExecutable(jobGroupName, JobGroup.class, s.toArray(new DistributableExecutable[]{}));
	}
}

package org.apache.dstream;

import java.util.stream.Stream;

public interface JobGroup extends DistributableExecutable<Stream<?>> {
	
	public static JobGroup create(String jobGroupName, DistributableExecutable<?>... distributable){
		return ExecutionContextSpecificationBuilder.asGroupExecutable(jobGroupName, JobGroup.class, distributable);
	}
	
//	private final String jobGroupName;
//	
//	
//
//	public JobGroup(String jobGroupName) {
//		Assert.notEmpty(jobGroupName, "'jobGroupName' must not be null or empty");
//		this.jobGroupName = jobGroupName;
//	}
//	
//	
//	public Future<Stream<Stream<?>>>[] execute(DistributableExecutable<?>... distributable) {
//		Assert.notEmpty(distributable, "'distributable' must contain at leas one element");
//		return null;
//	}
	
}

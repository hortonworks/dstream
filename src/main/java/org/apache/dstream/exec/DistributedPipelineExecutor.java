package org.apache.dstream.exec;

import org.apache.dstream.DistributedPipeline;
import org.apache.dstream.assembly.DistributedPipelineAssembly;

public abstract class DistributedPipelineExecutor<T,R> {

	protected final DistributedPipelineAssembly<T> streamAssembly;
	
	public DistributedPipelineExecutor(DistributedPipelineAssembly<T> streamAssembly){
		this.streamAssembly = streamAssembly;
	}
	
	public abstract DistributedPipeline<R> execute();
}

package org.apache.dstream.exec;

import org.apache.dstream.DataPipeline;
import org.apache.dstream.assembly.DistributedPipelineAssembly;

public abstract class DistributedPipelineExecutor<T,R> {

	protected final DistributedPipelineAssembly<T> streamAssembly;
	
	public DistributedPipelineExecutor(DistributedPipelineAssembly<T> streamAssembly){
		this.streamAssembly = streamAssembly;
	}
	
	public abstract DataPipeline<R> execute();
}

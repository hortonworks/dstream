package org.apache.dstream.local;

import org.apache.dstream.DistributedPipelineExecutionProvider;
import org.apache.dstream.exec.DistributedPipelineExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class DistributedPipelineExecutionProviderImpl<T> extends DistributedPipelineExecutionProvider<T> {
	
	private final Logger logger = LoggerFactory.getLogger(DistributedPipelineExecutionProviderImpl.class);
	
	public DistributedPipelineExecutionProviderImpl(){
		this.getSupportedProtocols().add("file");
		if (logger.isInfoEnabled()){
			logger.info("Created instance of StreamExecutionContext[" + this.getClass().getName() + "]; Supported protocols: " + this.getSupportedProtocols());
		}
	}

	@Override
	public <R> DistributedPipelineExecutor<T,R> getExecutor() {
		return new StreamExecutorImpl<T,R>(this.getAssembly());
	}
}

package org.apache.dstream.local;

import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.exec.StreamExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class StreamExecutionContextImpl<T> extends StreamExecutionContext<T> {
	
	private final Logger logger = LoggerFactory.getLogger(StreamExecutionContextImpl.class);
	
	public StreamExecutionContextImpl(){
		this.getSupportedProtocols().add("file");
		if (logger.isInfoEnabled()){
			logger.info("Created instance of StreamExecutionContext[" + this.getClass().getName() + "]; Supported protocols: " + this.getSupportedProtocols());
		}
	}

	@Override
	public <R> StreamExecutor<T,R> getStreamExecutor() {
		return new StreamExecutorImpl<T,R>(this.getStreamAssembly());
	}

	@Override
	protected void preProcessSource() {
		// TODO Auto-generated method stub
		
	}
}

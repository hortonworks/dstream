package org.apache.dstream.local;

import java.io.InputStream;
import java.util.stream.Stream;

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
		this.supportedProtocols.add("file");
		if (logger.isInfoEnabled()){
			logger.info("Created instance of StreamExecutionContext[" + this.getClass().getName() + "]; Supported protocols: " + this.supportedProtocols);
		}
	}

	@Override
	public InputStream toInputStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<T> stream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StreamExecutor<T> getStreamExecutor() {
		return new StreamExecutorImpl<T>(this.streamAssembly);
	}
}

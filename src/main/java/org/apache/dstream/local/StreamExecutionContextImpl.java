package org.apache.dstream.local;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.Submittable;
import org.apache.dstream.exec.StreamExecutor;
import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;
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
	public InputStream toInputStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<T> toStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
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

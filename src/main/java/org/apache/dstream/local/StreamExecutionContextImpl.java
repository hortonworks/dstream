package org.apache.dstream.local;

import java.io.InputStream;
import java.util.stream.Stream;

import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.exec.StreamExecutor;
import org.apache.dstream.io.FsStreamableSource;
import org.apache.dstream.io.ListStreamableSource;
import org.apache.dstream.io.StreamableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class StreamExecutionContextImpl<T> extends StreamExecutionContext<T> {
	
	private final Logger logger = LoggerFactory.getLogger(StreamExecutionContextImpl.class);
	
	private final String[] supportedProtocols = new String[]{"file"};

	@Override
	protected boolean isSourceSupported(StreamableSource<T> source) {
		if (source instanceof ListStreamableSource){
			return true;
		} else if (source instanceof FsStreamableSource) {
			@SuppressWarnings("rawtypes")
			String protocol = ((FsStreamableSource)source).getScheme();
			for (String supportedProtocol : this.supportedProtocols) {
				if (supportedProtocol.equals(protocol)){
					return true;
				}
			}
		}
		return false;
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

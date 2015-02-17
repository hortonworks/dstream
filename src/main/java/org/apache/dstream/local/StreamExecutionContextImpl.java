package org.apache.dstream.local;

import java.io.InputStream;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.StageEntryPoint;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.CollectionStreamableSource;
import org.apache.dstream.io.FsStreamableSource;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class StreamExecutionContextImpl<T> extends StreamExecutionContext<T> implements StageEntryPoint<T> {
	
	private final Logger logger = LoggerFactory.getLogger(StreamExecutionContextImpl.class);
	
	private final String[] supportedProtocols = new String[]{"file"};

	@Override
	public <K,V,R> IntermediateKVResult<K, V> computeKeyValue(Class<K> outputKey, Class<V> outputVal,
			SerializableFunction<Stream<T>, R> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computeKeyValue' request with output KEY/VALUE as " + 
					outputKey.getSimpleName() + "/" + outputVal.getSimpleName());
		}
		return new IntermediateKVResultImpl<K, V>();
	}

	@Override
	public <R> R compute(SerializableFunction<Stream<T>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean isSourceSupported(StreamableSource<T> source) {
		if (source instanceof CollectionStreamableSource){
			return true;
		} else if (source instanceof FsStreamableSource) {
			@SuppressWarnings("rawtypes")
			String protocol = ((FsStreamableSource)source).getPath().toUri().getScheme();
			for (String supportedProtocol : this.supportedProtocols) {
				if (supportedProtocol.equals(protocol)){
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public StreamableSource<T> getSource() {
		// TODO Auto-generated method stub
		return null;
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

}

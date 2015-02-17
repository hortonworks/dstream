package org.apache.dstream.local;

import java.io.InputStream;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.StageEntryPoint;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 * @param <T>
 */
public class StreamExecutionContextImpl<T> extends StreamExecutionContext<T> implements StageEntryPoint<T>{
	
	private final String[] supportedProtocols = new String[]{"file"};

	@Override
	public <K,V,R> IntermediateKVResult<K, V> computeKeyValue(Class<K> outputKey, Class<V> outputVal,
			SerializableFunction<Stream<T>, R> function) {
		// TODO Auto-generated method stub
		return new IntermediateKVResultImpl<K, V>();
	}

	@Override
	public <R> R compute(SerializableFunction<Stream<T>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean isSourceSupported(StreamableSource<T> source) {
		// TODO Auto-generated method stub
		return true;
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

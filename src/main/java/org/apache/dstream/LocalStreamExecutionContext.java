package org.apache.dstream;

import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 * @param <T>
 */
public class LocalStreamExecutionContext<T> extends StreamExecutionContext<T> implements StageEntryPoint<T>{
	
	private final String[] supportedProtocols = new String[]{"file"};

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
	
	@Override
	protected boolean isSourceSupported(StreamableSource<T> source) {
//		for (String supportedProtocol : this.supportedProtocols) {
//			if (supportedProtocol.equals(protocol)){
//				return true;
//			}
//		}
//		return false;
		return true;
	}

	@Override
	public <K, V, R> IntermediateKVResult<K, V> computeAsKeyValue(Class<K> outputKey, Class<V> outputVal,
			SerializableFunction<Stream<T>, Map<K, V>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> R compute(SerializableFunction<Stream<T>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}
}

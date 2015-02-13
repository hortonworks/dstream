package org.apache.dstream;

import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

public class TestStreamExecutionContext<T> extends StreamExecutionContext<T> {

	@Override
	public <K, V, R> org.apache.dstream.StreamExecutionContext.IntermediateKVResult<K, V> computeAsKeyValue(
			Class<K> outputKey,
			Class<V> outputVal,
			org.apache.dstream.StreamExecutionContext.SerializableFunction<Stream<T>, Map<K, V>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> R compute(
			org.apache.dstream.StreamExecutionContext.SerializableFunction<Stream<T>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Streamable<T> getSource() {
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

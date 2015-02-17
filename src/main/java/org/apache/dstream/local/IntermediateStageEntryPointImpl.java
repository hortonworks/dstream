package org.apache.dstream.local;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.IntermediateStageEntryPoint;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.utils.SerializableFunction;

public class IntermediateStageEntryPointImpl<T> implements IntermediateStageEntryPoint<T> {

	@Override
	public <K, V> IntermediateKVResult<K, V> computeKeyValue(Class<K> outputKey, Class<V> outputVal,
			SerializableFunction<Stream<T>, Map<K, V>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> R compute(SerializableFunction<Stream<T>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StreamExecutionContext<T> saveAs(OutputSpecification outputSpec) {
		// TODO Auto-generated method stub
		return null;
	}

}

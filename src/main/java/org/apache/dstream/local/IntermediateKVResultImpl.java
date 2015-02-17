package org.apache.dstream.local;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.IntermediateStageEntryPoint;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;

public class IntermediateKVResultImpl<K, V> implements IntermediateKVResult<K,V> {

	@Override
	public IntermediateStageEntryPoint<Entry<K, V>> partition(int partitionSize, BinaryOperator<V> mergeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IntermediateStageEntryPoint<Entry<K, V>> partition(Partitioner partitioner, BinaryOperator<V> mergeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IntermediateStageEntryPoint<Entry<K, V>> partition(SerializableFunction<Entry<K, V>, Integer> partitionerFunction,
			BinaryOperator<V> mergeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StreamExecutionContext<Entry<K, V>> saveAs(OutputSpecification outputSpec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <NK,NV,R> IntermediateKVResult<NK, NV> computeKeyValue(Class<NK> outputKey, Class<NV> outputVal,
			SerializableFunction<Stream<Entry<K, V>>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> R compute(SerializableFunction<Stream<Entry<K, V>>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}
}

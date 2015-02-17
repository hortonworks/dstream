package org.apache.dstream.local;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.Submittable;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntermediateKVResultImpl<K, V> implements IntermediateKVResult<K,V> {
	
	private final Logger logger = LoggerFactory.getLogger(IntermediateKVResultImpl.class);

	@Override
	public Submittable<Entry<K, V>> partition(int partitionSize, BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'partition' request for " + partitionSize + " partitions and merge function.");
		}
		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
	}

	@Override
	public Submittable<Entry<K, V>> partition(Partitioner partitioner, BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'partition' request with " + partitioner + " and merge function.");
		}
		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
	}

	@Override
	public Submittable<Entry<K, V>> partition(SerializableFunction<Entry<K, V>, Integer> partitionerFunction,
			BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'partition' request with partitioner function and merge function.");
		}
		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
	}

	@Override
	public StreamExecutionContext<Entry<K, V>> saveAs(OutputSpecification outputSpec) {
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		return null;
	}

	@Override
	public <NK,NV,R> IntermediateKVResult<NK, NV> computeKeyValue(Class<NK> outputKey, Class<NV> outputVal,
			SerializableFunction<Stream<Entry<K, V>>, R> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computeKeyValue' request with output KEY/VALUE as " + 
					outputKey.getSimpleName() + "/" + outputVal.getSimpleName());
		}
		return new IntermediateKVResultImpl<NK,NV>();
	}

	@Override
	public <R> R compute(SerializableFunction<Stream<Entry<K, V>>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}
}

package org.apache.dstream.local;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.Submittable;
import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link IntermediateKVResult}
 * 
 * @param <K>
 * @param <V>
 */
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
}

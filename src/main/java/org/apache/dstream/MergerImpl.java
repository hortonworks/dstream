package org.apache.dstream;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Merger}
 * 
 * @param <K>
 * @param <V>
 */
public class MergerImpl<K, V> implements Merger<K,V> {
	
	private final Logger logger = LoggerFactory.getLogger(MergerImpl.class);

	@Override
	public Submittable<Entry<K, V>> merge(int partitionSize, BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'merge' request for " + partitionSize + " partitions and merge function.");
		}
		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
	}

	@Override
	public Submittable<Entry<K, V>> merge(Partitioner partitioner, BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'merge' request with " + partitioner + " and merge function.");
		}
		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
	}

	@Override
	public Submittable<Entry<K, V>> merge(SerializableFunction<Entry<K, V>, Integer> partitionerFunction,
			BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'merge' request with partitioner function and merge function.");
		}
		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
	}
}

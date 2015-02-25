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
	
	private static final long serialVersionUID = 7020089231859026667L;

	private final Logger logger = LoggerFactory.getLogger(MergerImpl.class);
	
	private transient final StreamExecutionContext<Entry<K, V>> executionContext;
	
	private int partitionSize;

	private BinaryOperator<V> mergeFunction;
	
	private SerializableFunction<Entry<K, V>, Integer> partitionerFunction;
	
	/**
	 * 
	 * @param context
	 */
	protected MergerImpl(StreamExecutionContext<Entry<K,V>> executionContext){
		this.executionContext = executionContext;
	}

	@Override
	public Submittable<Entry<K, V>> merge(int partitionSize, BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'merge' request for " + partitionSize + " partitions.");
		}
		this.partitionSize = partitionSize;
		this.mergeFunction = mergeFunction;
		Partitioner defaultPartitioner = new DefaultPartitioner(this.partitionSize);
		this.partitionerFunction = new SerializableFunction<Entry<K, V>, Integer>() {
			private static final long serialVersionUID = -8996083508793084950L;
			@Override
			public Integer apply(Entry<K, V> t) {
				return defaultPartitioner.getPartition(t);
			}
		};
		this.executionContext.streamAssembly.getLastStage().setMerger(this);
		return new IntermediateStageEntryPointImpl<Entry<K,V>>(this.executionContext);
	}

	@Override
	public Submittable<Entry<K, V>> merge(Partitioner partitioner, BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'merge' request with " + partitioner + ".");
		}
		this.partitionerFunction = new SerializableFunction<Entry<K, V>, Integer>() {
			private static final long serialVersionUID = 6530880100257370609L;
			@Override
			public Integer apply(Entry<K, V> t) {
				return partitioner.getPartition(t);
			}
		};
		this.mergeFunction = mergeFunction;
		this.executionContext.streamAssembly.getLastStage().setMerger(this);
		return new IntermediateStageEntryPointImpl<Entry<K,V>>(this.executionContext);
	}

	@Override
	public Submittable<Entry<K, V>> merge(SerializableFunction<Entry<K, V>, Integer> partitionerFunction,
			BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'merge' request with partitioner function.");
		}
		this.partitionerFunction = partitionerFunction;
		this.mergeFunction = mergeFunction;
		this.executionContext.streamAssembly.getLastStage().setMerger(this);
		return new IntermediateStageEntryPointImpl<Entry<K,V>>(this.executionContext);
	}
	
	public int getPartitionSize() {
		return partitionSize;
	}

	public BinaryOperator<V> getMergeFunction() {
		return mergeFunction;
	}

	public SerializableFunction<Entry<K, V>, Integer> getPartitionerFunction() {
		return partitionerFunction;
	}
	
	/**
	 * 
	 */
	private class DefaultPartitioner extends Partitioner<K,V> {
		private static final long serialVersionUID = -7960042449579121912L;

		public DefaultPartitioner(int partitionSize) {
			super(partitionSize);
		}

		@Override
		public int getPartition(Entry<K,V> input) {
			return (input.getKey().hashCode() & Integer.MAX_VALUE) % partitionSize;
		}
		
	}
}

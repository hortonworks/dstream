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
			logger.debug("Accepted 'merge' request for " + partitionSize + " partitions and merge function.");
		}
		this.partitionSize = partitionSize;
		this.mergeFunction = mergeFunction;
		this.executionContext.streamAssembly.getLastStage().setMerger(this);
		return new IntermediateStageEntryPointImpl<Entry<K,V>>(this.executionContext);
	}

	@Override
	public Submittable<Entry<K, V>> merge(Partitioner partitioner, BinaryOperator<V> mergeFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'merge' request with " + partitioner + " and merge function.");
		}
		this.partitionerFunction = new SerializableFunction<Entry<K,V>, Integer>() {
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
			logger.debug("Accepted 'merge' request with partitioner function and merge function.");
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
}

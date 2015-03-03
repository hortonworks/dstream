package org.apache.dstream;

import java.util.Map.Entry;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableBiFunction;
import org.apache.dstream.utils.SerializableBinaryOperator;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link IntermediateResult}
 * 
 * @param <K>
 * @param <V>
 */
public class IntermediateResultImpl<K, V> implements IntermediateResult<K,V> {
	
	private static final long serialVersionUID = 7020089231859026667L;

	private final Logger logger = LoggerFactory.getLogger(IntermediateResultImpl.class);
	
	private transient final DistributedPipelineExecutionProvider<Entry<K, V>> executionContext;
	
	private int partitionSize;

	private SerializableBinaryOperator<V> aggregateFunction;
	
	private SerializableFunction<Entry<K, V>, Integer> partitionerFunction;
	
	/**
	 * 
	 * @param context
	 */
	protected IntermediateResultImpl(DistributedPipelineExecutionProvider<Entry<K,V>> executionContext){
		this.executionContext = executionContext;
	}

	@Override
	public Submittable<Entry<K, V>> aggregate(int partitionSize, SerializableBinaryOperator<V> aggregateFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'aggregate' request for " + partitionSize + " partitions.");
		}
		this.partitionSize = partitionSize;
		this.aggregateFunction = aggregateFunction;
		Partitioner<Entry<K, V>> defaultPartitioner = new DefaultPartitioner(this.partitionSize);
		this.partitionerFunction = new SerializableFunction<Entry<K, V>, Integer>() {
			private static final long serialVersionUID = -8996083508793084950L;
			@Override
			public Integer apply(Entry<K, V> t) {
				return defaultPartitioner.getPartition(t);
			}
		};
		this.executionContext.getAssembly().getLastStage().setMerger(this);
		return new IntermediateStageEntryPointImpl<Entry<K,V>>(this.executionContext);
	}

	@Override
	public Submittable<Entry<K, V>> aggregate(Partitioner<Entry<K,V>> partitioner, SerializableBinaryOperator<V> aggregateFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'aggregate' request with " + partitioner + ".");
		}
		this.partitionerFunction = new SerializableFunction<Entry<K, V>, Integer>() {
			private static final long serialVersionUID = 6530880100257370609L;
			@Override
			public Integer apply(Entry<K, V> t) {
				return partitioner.getPartition(t);
			}
		};
		this.aggregateFunction = aggregateFunction;
		this.executionContext.getAssembly().getLastStage().setMerger(this);
		return new IntermediateStageEntryPointImpl<Entry<K,V>>(this.executionContext);
	}

	@Override
	public Submittable<Entry<K, V>> aggregate(SerializableFunction<Entry<K, V>, Integer> partitionerFunction,
			SerializableBinaryOperator<V> aggregateFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'aggregate' request with partitioner function.");
		}
		this.partitionerFunction = partitionerFunction;
		this.aggregateFunction = aggregateFunction;
		this.executionContext.getAssembly().getLastStage().setMerger(this);
		return new IntermediateStageEntryPointImpl<Entry<K,V>>(this.executionContext);
	}
	
	public int getPartitionSize() {
		return partitionSize;
	}

	public SerializableBinaryOperator<V> getMergeFunction() {
		return aggregateFunction;
	}

	public SerializableFunction<Entry<K, V>, Integer> getPartitionerFunction() {
		return partitionerFunction;
	}
	
	/**
	 * 
	 */
	private class DefaultPartitioner extends Partitioner<Entry<K,V>> {
		private static final long serialVersionUID = -7960042449579121912L;

		public DefaultPartitioner(int partitionSize) {
			super(partitionSize);
		}

		@Override
		public int getPartition(Entry<K,V> input) {
			return (input.getKey().hashCode() & Integer.MAX_VALUE) % partitionSize;
		}
	}

	@Override
	public Submittable<Entry<K, V>> partition(int partitionSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Submittable<Entry<K, V>> partition(Partitioner<Entry<K,V>> partitioner) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Submittable<Entry<K, V>> partition(SerializableFunction<Entry<K, V>, Integer> partitionerFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> IntermediateResult<K, Entry<V,R>> join(IntermediateResult<K, R> intermediateResult) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <KK, VV, R> IntermediateResult<K, R> join(IntermediateResult<KK, VV> intermediateResult,
			SerializableBiFunction<V, VV, R> valueCombiner) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IntermediateResult<K, Iterable<V>> groupByKey() {
		// TODO Auto-generated method stub
		return null;
	}
}
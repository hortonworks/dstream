package org.apache.dstream.local;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.IntermediateResult;
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
	public StreamExecutionContext<Entry<K, V>> saveAs(
			OutputSpecification outputSpec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int computeInt(
			SerializableFunction<Stream<Entry<K, V>>, Integer> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeLong(
			SerializableFunction<Stream<Entry<K, V>>, Long> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double computeDouble(
			SerializableFunction<Stream<Entry<K, V>>, Double> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean computeBoolean(
			SerializableFunction<Stream<Entry<K, V>>, Boolean> function) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <R> IntermediateResult<R> computeCollection(
			SerializableFunction<Stream<Entry<K, V>>, Collection<R>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <C, W> IntermediateKVResult<C, W> computePairs(
			SerializableFunction<Stream<Entry<K, V>>, Map<C, W>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Submittable<Entry<K, V>> partition(int partitionSize,
			BinaryOperator<V> mergeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Submittable<Entry<K, V>> partition(Partitioner partitioner,
			BinaryOperator<V> mergeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Submittable<Entry<K, V>> partition(
			SerializableFunction<Entry<K, V>, Integer> partitionerFunction,
			BinaryOperator<V> mergeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	
//	@Override
//	public Submittable<Entry<K, V>> partition(int partitionSize, BinaryOperator<V> mergeFunction) {
//		if (logger.isDebugEnabled()){
//			logger.debug("Accepted 'partition' request for " + partitionSize + " partitions and merge function.");
//		}
//		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
//	}
//
//	@Override
//	public Submittable<Entry<K, V>> partition(Partitioner partitioner, BinaryOperator<V> mergeFunction) {
//		if (logger.isDebugEnabled()){
//			logger.debug("Accepted 'partition' request with " + partitioner + " and merge function.");
//		}
//		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
//	}
//
//	@Override
//	public Submittable<Entry<K, V>> partition(SerializableFunction<Entry<K, V>, Integer> partitionerFunction,
//			BinaryOperator<V> mergeFunction) {
//		if (logger.isDebugEnabled()){
//			logger.debug("Accepted 'partition' request with partitioner function and merge function.");
//		}
//		return new IntermediateStageEntryPointImpl<Entry<K,V>>();
//	}
//
//	@Override
//	public StreamExecutionContext<Entry<K, V>> saveAs(OutputSpecification outputSpec) {
//		if (logger.isDebugEnabled()){
//			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
//		}
//		return null;
//	}
//
//	@Override
//	public <NK,NV,R> IntermediateKVResult<NK, NV> computeKeyValue(Class<NK> outputKey, Class<NV> outputVal,
//			SerializableFunction<Stream<Entry<K, V>>, R> function) {
//		if (logger.isDebugEnabled()){
//			logger.debug("Accepted 'computeKeyValue' request with output KEY/VALUE as " + 
//					outputKey.getSimpleName() + "/" + outputVal.getSimpleName());
//		}
//		return new IntermediateKVResultImpl<NK,NV>();
//	}
//
//	@Override
//	public <R> R compute(SerializableFunction<Stream<Entry<K, V>>, R> function) {
//		// TODO Auto-generated method stub
//		return null;
//	}
}

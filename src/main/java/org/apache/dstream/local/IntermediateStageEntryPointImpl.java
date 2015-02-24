package org.apache.dstream.local;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.dstream.Merger;
import org.apache.dstream.IntermediateResult;
import org.apache.dstream.Submittable;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class IntermediateStageEntryPointImpl<T> implements Submittable<T> {
	
	private final Logger logger = LoggerFactory.getLogger(IntermediateStageEntryPointImpl.class);

	@Override
	public int computeInt(SerializableFunction<Stream<T>, Integer> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeLong(SerializableFunction<Stream<T>, Long> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double computeDouble(SerializableFunction<Stream<T>, Double> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean computeBoolean(
			SerializableFunction<Stream<T>, Boolean> function) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <R> IntermediateResult<R> computeCollection(SerializableFunction<Stream<T>, Collection<R>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> Merger<K, V> computePairs(SerializableFunction<Stream<T>, Map<K, V>> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computePairs' request");
		}
		return new MergerImpl<K, V>();
	}

	@Override
	public Stream<T> saveAs(OutputSpecification outputSpec) {
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		return null;
	}
}

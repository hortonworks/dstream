package org.apache.dstream.local;

import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.Submittable;
import org.apache.dstream.StreamExecutionContext;
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
	public <K,V,R> IntermediateKVResult<K, V> computeKeyValue(Class<K> outputKey, Class<V> outputVal,
			SerializableFunction<Stream<T>, R> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computeKeyValue' request with output KEY/VALUE as " + 
					outputKey.getSimpleName() + "/" + outputVal.getSimpleName());
		}
		return new IntermediateKVResultImpl<K, V>();
	}

	@Override
	public <R> R compute(SerializableFunction<Stream<T>, R> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StreamExecutionContext<T> saveAs(OutputSpecification outputSpec) {
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		return null;
	}

}

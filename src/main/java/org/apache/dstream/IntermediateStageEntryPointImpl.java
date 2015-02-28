package org.apache.dstream;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.exec.StreamExecutor;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.utils.NullType;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class IntermediateStageEntryPointImpl<T> implements Submittable<T> {
	
	private final Logger logger = LoggerFactory.getLogger(IntermediateStageEntryPointImpl.class);
	
	private final StreamExecutionContext<T> executionContext;
	
	protected IntermediateStageEntryPointImpl(StreamExecutionContext<T> executionContext){
		this.executionContext = executionContext;
	}

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
	public <R> IntermediateResult<NullType, R> computeCollection(SerializableFunction<Stream<T>, Collection<R>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> IntermediateResult<K, V> computePairs(SerializableFunction<Stream<T>, Map<K, V>> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computePairs' request");
		}
		return new IntermediateResultImpl<K, V>((StreamExecutionContext<Entry<K, V>>) this.executionContext);
	}

	@Override
	public Stream<T> save(OutputSpecification outputSpec) {
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		this.executionContext.getStreamAssembly().setOutputSpecification(outputSpec);
		StreamExecutor<?, T> streamExecutor = this.executionContext.getStreamExecutor();
		return streamExecutor.execute();
	}

	@Override
	public Stream<T> collect() {
		// TODO Auto-generated method stub
		return null;
	}
}

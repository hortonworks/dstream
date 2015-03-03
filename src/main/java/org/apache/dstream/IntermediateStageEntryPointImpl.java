package org.apache.dstream;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.exec.DistributedPipelineExecutor;
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
	
	private final DistributedPipelineExecutionProvider<T> executionProvider;
	
	protected IntermediateStageEntryPointImpl(DistributedPipelineExecutionProvider<T> executionProvider){
		this.executionProvider = executionProvider;
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
	public <V> IntermediateResult<T, V> computePairs(SerializableFunction<Stream<T>, Map<T, V>> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computePairs' request");
		}
		return new IntermediateResultImpl<T, V>((DistributedPipelineExecutionProvider<Entry<T, V>>) this.executionProvider);
	}

	@Override
	public DistributedPipeline<T> save(OutputSpecification outputSpec) {
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		this.executionProvider.getAssembly().setOutputSpecification(outputSpec);
		DistributedPipelineExecutor<?, T> streamExecutor = this.executionProvider.getExecutor();
		return streamExecutor.execute();
	}

	@Override
	public DistributedPipeline<T> save(FileSystem fs) {
		OutputSpecification outputSpec = new OutputSpecificationImpl(fs.getPath(this.executionProvider.getAssembly().getJobName() + "/out"));
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		this.executionProvider.getAssembly().setOutputSpecification(outputSpec);
		DistributedPipelineExecutor<?, T> streamExecutor = this.executionProvider.getExecutor();
		return streamExecutor.execute();
	}
}

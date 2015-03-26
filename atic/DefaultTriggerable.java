package org.apache.dstream;

import java.nio.file.FileSystem;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.exec.DistributedPipelineExecutor;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class DefaultTriggerable<T> implements Triggerable<T>, Computable<T> {
	
	private final Logger logger = LoggerFactory.getLogger(DefaultTriggerable.class);
	
	private final AbstractDataPipelineExecutionProvider<T> executionProvider;
	
	protected DefaultTriggerable(AbstractDataPipelineExecutionProvider<T> executionProvider){
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
	public boolean computeBoolean(SerializableFunction<Stream<T>, Boolean> function) {
		// TODO Auto-generated method stub
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K,V> Distributable<K, V> computeMappings(SerializableFunction<Stream<T>, Map<K, V>> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computePairs' request");
		}
		return new DefaultDistributable<K, V>((AbstractDataPipelineExecutionProvider<Entry<K, V>>) this.executionProvider);
	}

	@Override
	public Pipeline<T> save(OutputSpecification outputSpec) {
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		this.executionProvider.getAssembly().setOutputSpecification(outputSpec);
		DistributedPipelineExecutor<?, T> streamExecutor = this.executionProvider.getExecutor();
		return streamExecutor.execute();
	}

	@Override
	public Pipeline<T> save(FileSystem fs) {
		OutputSpecification outputSpec = new SimpleOutputSpecification(fs.getPath(this.executionProvider.getAssembly().getJobName() + "/out"));
		if (logger.isDebugEnabled()){
			logger.debug("Submitting job. Output will be available at: " + outputSpec.getOutputPath());
		}
		this.executionProvider.getAssembly().setOutputSpecification(outputSpec);
		DistributedPipelineExecutor<?, T> streamExecutor = this.executionProvider.getExecutor();
		return streamExecutor.execute();
	}

	@Override
	public long count() {
		// TODO Auto-generated method stub
		return 0;
	}

}

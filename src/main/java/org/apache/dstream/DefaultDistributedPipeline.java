package org.apache.dstream;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.assembly.Stage;
import org.apache.dstream.utils.NullType;
import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedPipeline<T> extends AbstractDistributedPipeline<T> {
	protected final Logger logger = LoggerFactory.getLogger(DefaultDistributedPipeline.class);
	
	private final DistributedPipelineExecutionProvider<T> executionContext;
	
	private final Source<T> source;
	
	public DefaultDistributedPipeline(Source<T> source, String jobName){
		this.source = source;
		this.executionContext = DistributedPipelineExecutionProvider.of(jobName, this);
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

		Stage<T> stage = new Stage<T>(function, this.executionContext.getSourcePreProcessFunction(), this.executionContext.nextStageId());
		this.executionContext.getAssembly().addStage(stage);
	
		return new DefaultDistributable<K, V>((DistributedPipelineExecutionProvider<Entry<K, V>>) this.executionContext);
	}

	@Override
	public Stream<T> toStream() {
		return this.source.toStream();
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Source<T> getSource() {
		return source;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "[" + this.source.toString() + "]";
	}
	
	@Override
	protected DistributedPipeline<T> preProcessSource(SerializableFunction<Path[], Path[]> sourcePreProcessFunction) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Persistable<T> partition(int partitionSize) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Persistable<T> partition(Partitioner<T> partitioner) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Persistable<T> partition(
			SerializableFunction<T, Integer> partitionerFunction) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
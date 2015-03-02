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

public class DistributableSourceImpl<T> extends AbstractDistributableSource<T> {
	protected final Logger logger = LoggerFactory.getLogger(DistributableSourceImpl.class);
	
	private final StreamExecutionContext<T> executionContext;
	
	public DistributableSourceImpl(){
		this.executionContext = StreamExecutionContext.of("foo", this);
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

		Stage<T> stage = new Stage<T>(function, this.executionContext.getSource().getPreprocessFunction(), this.executionContext.nextStageId());
		this.executionContext.getStreamAssembly().addStage(stage);
	
		return new IntermediateResultImpl<T, V>((StreamExecutionContext<Entry<T, V>>) this.executionContext);
	}
	
	@Override
	public Submittable<T> partition(int partitionSize) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Submittable<T> partition(Partitioner<T> partitioner) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Submittable<T> partition(
			SerializableFunction<T, Integer> partitionerFunction) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Stream<T> toStream() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected DistributableSource<T> preProcessSource(SerializableFunction<Path[], Path[]> sourcePreProcessFunction) {
		// TODO Auto-generated method stub
		return null;
	}
}
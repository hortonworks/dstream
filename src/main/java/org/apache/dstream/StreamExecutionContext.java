package org.apache.dstream;

import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.dstream.assembly.Stage;
import org.apache.dstream.assembly.StreamAssembly;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.ReflectionUtils;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Base class which defines <b>Execution Context</b> for distributing and executing 
 * Java {@link Stream}s.
 *
 * @param <T>
 */
public abstract class StreamExecutionContext<T> implements StageEntryPoint<T> {
	
	private static final Logger logger = LoggerFactory.getLogger(StreamExecutionContext.class);
	
	private volatile StreamableSource<T> source;
	
	protected final StreamAssembly dagContext = ReflectionUtils.newDefaultInstance(StreamAssembly.class);
	
	
	/**
	 * Factory method that will return implementation of this {@link StreamExecutionContext}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> StreamExecutionContext<T> of(String jobName, StreamableSource<T> source) {
		Assert.notNull(source, "Streamable source must not be null");
		Assert.notEmpty(jobName, "'jobName' must not be null or empty");
		
		ServiceLoader<StreamExecutionContext> sl = ServiceLoader.load(StreamExecutionContext.class, ClassLoader.getSystemClassLoader());
		Iterator<StreamExecutionContext> iter = sl.iterator();
		StreamExecutionContext<T> suitableContext = null;
		while (iter.hasNext() && suitableContext == null){
			StreamExecutionContext context = iter.next();
			if (context.isSourceSupported(source)){
				if (logger.isInfoEnabled()){
					logger.info("Loading execution context: " + context + " which supports '" + source);
				}
				suitableContext = context;
			} else {
				if (logger.isInfoEnabled()){
					logger.info("Available context: " + context + " will not be loaded since it does not "
							+ "support '" +  source);
				}
			}
		}
		if (suitableContext == null){
			throw new IllegalStateException("No suitable execution context was found");
		}
		
		ReflectionUtils.setFieldValue(suitableContext.dagContext, "jobName", jobName);
		if (logger.isDebugEnabled()){
			logger.debug("DagContext jobName: " + jobName);
		}
		ReflectionUtils.setFieldValue(suitableContext.dagContext, "source", source);
		if (logger.isDebugEnabled()){
			logger.debug("DagContext source: " + source);
		}
		
		return suitableContext;
	}
	
	/**
	 * 
	 * @return
	 */
	public StreamableSource<T> getSource() {
		return this.source;
	}
	
	@Override
	public <K, V> Merger<K, V> computePairs(SerializableFunction<Stream<T>, Map<K, V>> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computePairs' request");
		}
		
		Stage stage = new Stage(function);
		this.dagContext.addStage(stage);
	
		return new MergerImpl<K, V>(this);
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
	public <R> IntermediateResult<R> computeCollection(SerializableFunction<Stream<T>, Collection<R>> function) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * 
	 */
	public String toString(){
		return this.getClass().getSimpleName();
	}
	
	/**
	 * 
	 * @param source
	 * @return
	 */
	protected abstract boolean isSourceSupported(StreamableSource<T> source);
	
	/**
	 * Returns the raw {@link InputStream} to the result data set.
	 * @return
	 */
	//http://stackoverflow.com/questions/22919013/inputstream-to-hadoop-sequencefile
	public abstract InputStream toInputStream(); 
	
	/**
	 * Returns the {@link Stream} to the result data set allowing result data to be streamed for 
	 * local processing (e.g., iterate over results)
	 * 
	 * @return
	 */
	public abstract Stream<T> stream();
	
	
	//public abstract void submit();
}

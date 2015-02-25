package org.apache.dstream;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.dstream.assembly.Stage;
import org.apache.dstream.assembly.StreamAssembly;
import org.apache.dstream.exec.StreamExecutor;
import org.apache.dstream.io.FsStreamableSource;
import org.apache.dstream.io.ListStreamableSource;
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
	
	protected final StreamAssembly streamAssembly = ReflectionUtils.newDefaultInstance(StreamAssembly.class);
	
	protected final List<String> supportedProtocols = new ArrayList<String>();
	
	
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
					logger.info("Loaded " + context + " to process source: " + source);
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
		
		ReflectionUtils.setFieldValue(suitableContext.streamAssembly, "jobName", jobName);
		if (logger.isDebugEnabled()){
			logger.debug("StreamAssembly jobName: " + jobName);
		}
		ReflectionUtils.setFieldValue(suitableContext.streamAssembly, "source", source);
		if (logger.isDebugEnabled()){
			logger.debug("StreamAssembly source: " + source);
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
	
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Merger<K, V> computePairs(SerializableFunction<Stream<T>, Map<K, V>> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computePairs' request");
		}
		
		Stage stage = new Stage(function);
		this.streamAssembly.addStage(stage);
	
		return new MergerImpl<K, V>((StreamExecutionContext<Entry<K, V>>) this);
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
	protected boolean isSourceSupported(StreamableSource<T> source){
		if (source instanceof ListStreamableSource){
			return true;
		} else if (source instanceof FsStreamableSource) {
			@SuppressWarnings("rawtypes")
			String protocol = ((FsStreamableSource)source).getScheme();
			for (String supportedProtocol : this.supportedProtocols) {
				if (supportedProtocol.equals(protocol)){
					return true;
				}
			}
		}
		return false;
	}
	
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
	
	
	public abstract StreamExecutor<T> getStreamExecutor();
}

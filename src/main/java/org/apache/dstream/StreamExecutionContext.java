package org.apache.dstream;

import java.io.InputStream;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.dstream.dag.DagContext;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.ReflectionUtils;
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
	
	protected volatile StreamableSource<T> source;
	
	protected final DagContext dagContext = ReflectionUtils.newDefaultInstance(DagContext.class);
	
	
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
	 * Returns the source of this stream as {@link StreamableSource}
	 * 
	 * @return
	 */
	public abstract StreamableSource<T> getSource();
	
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
}

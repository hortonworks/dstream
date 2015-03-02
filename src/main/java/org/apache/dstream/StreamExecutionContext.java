package org.apache.dstream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.dstream.assembly.StreamAssembly;
import org.apache.dstream.exec.StreamExecutor;
import org.apache.dstream.io.FsSource;
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
public abstract class StreamExecutionContext<T> {//implements StageEntryPoint<T>, Partitionable<T>, Closeable {
	
	private static final Logger logger = LoggerFactory.getLogger(StreamExecutionContext.class);
	
	private volatile AbstractDistributableSource<T> source;
	
	@SuppressWarnings("unchecked")
	private final StreamAssembly<T> streamAssembly = ReflectionUtils.newDefaultInstance(StreamAssembly.class);

	private final List<String> supportedProtocols = new ArrayList<String>();

	private int stageIdCounter;
	
	
	/**
	 * Factory method that will return implementation of this {@link StreamExecutionContext}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static <T> StreamExecutionContext<T> of(String jobName, AbstractDistributableSource<T> source) {
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
		
		suitableContext.source = source;
		
		suitableContext.preProcessSource();
		
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
	 */
	public String toString(){
		return this.getClass().getSimpleName();
	}
	
	protected int nextStageId(){
		return stageIdCounter++;
	}
	
	protected StreamAssembly<T> getStreamAssembly() {
		return streamAssembly;
	}
	
	protected List<String> getSupportedProtocols() {
		return supportedProtocols;
	}
	
	/**
	 * 
	 * @return
	 */
	protected AbstractDistributableSource<T> getSource() {
		return this.source;
	}
	
	/**
	 * 
	 * @param source
	 * @return
	 */
	protected boolean isSourceSupported(DistributableSource<T> source){
		if (source instanceof FsSource) {
			@SuppressWarnings("rawtypes")
			String protocol = ((FsSource)source).getScheme();
			for (String supportedProtocol : this.supportedProtocols) {
				if (supportedProtocol.equals(protocol)){
					return true;
				}
			}
		} else {
			throw new IllegalArgumentException("Unsupported source type: " + source);
		}
		return false;
	}
	
//	/**
//	 * Returns the raw {@link InputStream} to the result data set.
//	 * @return
//	 */
//	//http://stackoverflow.com/questions/22919013/inputstream-to-hadoop-sequencefile
//	public abstract InputStream toInputStream(); 
	
//	/**
//	 * Returns the {@link Stream} to the result data set allowing result data to be streamed for 
//	 * local processing (e.g., iterate over results)
//	 * 
//	 * @return
//	 */
//	public abstract Stream<T> toStream();
	
	protected abstract <R> StreamExecutor<T,R> getStreamExecutor();
	
	/**
	 * Will compose final pipeline by prepending additional pre-processing function to the pipeline.
	 * Such function is typically a map function. For example in the case of TextSource which 
	 * corresponds to TextInputFormat in Hadoop, such function may map <LongWritable, Text> to String.
	 * 
	 * If no function can be located, nothing will be prepended to the main pipeline.
	 */
	protected abstract void preProcessSource();
	
}

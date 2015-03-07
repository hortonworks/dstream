package org.apache.dstream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.dstream.assembly.DistributedPipelineAssembly;
import org.apache.dstream.exec.DistributedPipelineExecutor;
import org.apache.dstream.io.FsSource;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.ReflectionUtils;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Base class which implements . . .
 *
 * @param <T>
 */
public abstract class AbstractDistributedPipelineExecutionProvider<T> {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractDistributedPipelineExecutionProvider.class);
	
	@SuppressWarnings("unchecked")
	private final DistributedPipelineAssembly<T> streamAssembly = ReflectionUtils.newDefaultInstance(DistributedPipelineAssembly.class);

	private final List<String> supportedProtocols = new ArrayList<String>();

	private int stageIdCounter;
	
	
	/**
	 * Factory method that will return implementation of this {@link AbstractDistributedPipelineExecutionProvider}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static <T> AbstractDistributedPipelineExecutionProvider<T> of(String jobName, AbstractDistributedPipeline<T> pipeline) {
		Assert.notNull(pipeline, "'pipeline' must not be null");
		Assert.notEmpty(jobName, "'jobName' must not be null or empty");
		
		ServiceLoader<AbstractDistributedPipelineExecutionProvider> sl = ServiceLoader.load(AbstractDistributedPipelineExecutionProvider.class, ClassLoader.getSystemClassLoader());
		Iterator<AbstractDistributedPipelineExecutionProvider> iter = sl.iterator();
		AbstractDistributedPipelineExecutionProvider<T> suitableContext = null;
		while (iter.hasNext() && suitableContext == null){
			AbstractDistributedPipelineExecutionProvider context = iter.next();
			if (context.isSourceSupported(pipeline)){
				if (logger.isInfoEnabled()){
					logger.info("Loaded " + context + " to process source: " + pipeline);
				}
				suitableContext = context;
			} else {
				if (logger.isInfoEnabled()){
					logger.info("Available context: " + context + " will not be loaded since it does not "
							+ "support '" +  pipeline);
				}
			}
		}
		if (suitableContext == null){
			throw new IllegalStateException("No suitable execution provider found to process: " + pipeline);
		}
		ReflectionUtils.setFieldValue(suitableContext.streamAssembly, "pipeline", pipeline);
//		suitableContext.source = source;
		
//		SerializableFunction<Stream<?>, Stream<?>> sourcePreprocessFunction = suitableContext.getSourcePreProcessFunction();
//		if (sourcePreprocessFunction != null){
//			pipeline.setPreprocessFunction(sourcePreprocessFunction);
//		}
//		suitableContext.preProcessSource();
		
		ReflectionUtils.setFieldValue(suitableContext.streamAssembly, "jobName", jobName);
		if (logger.isDebugEnabled()){
			logger.debug("StreamAssembly jobName: " + jobName);
		}
//		ReflectionUtils.setFieldValue(suitableContext.streamAssembly, "source", source);
//		if (logger.isDebugEnabled()){
//			logger.debug("StreamAssembly source: " + source);
//		}
		
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
	
	protected DistributedPipelineAssembly<T> getAssembly() {
		return streamAssembly;
	}
	
	protected List<String> getSupportedProtocols() {
		return supportedProtocols;
	}
	
//	/**
//	 * 
//	 * @return
//	 */
//	protected AbstractDistributedPipeline<T> getSource() {
//		return this.source;
//	}
	
	/**
	 * 
	 * @param source
	 * @return
	 */
	protected boolean isSourceSupported(DataPipeline<T> pipeline){
		Source<T> source = pipeline.getSource();
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
	
	protected abstract <R> DistributedPipelineExecutor<T,R> getExecutor();
	
	/**
	 * Will compose final pipeline by prepending additional pre-processing function to the pipeline.
	 * Such function is typically a map function. For example in the case of TextSource which 
	 * corresponds to TextInputFormat in Hadoop, such function may map <LongWritable, Text> to String.
	 * 
	 * If no function can be located, nothing will be prepended to the main pipeline.
	 */
	protected SerializableFunction<Stream<?>, Stream<?>> getSourcePreProcessFunction() {
		return null;
	}
}

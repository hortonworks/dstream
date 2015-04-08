package org.apache.dstream.local;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.dstream.PipelineSpecification;
import org.apache.dstream.PipelineSpecification.Stage;
import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class LocalPipelineDelegate {
	
	private final Logger logger = LoggerFactory.getLogger(LocalPipelineDelegate.class);
	
	private Properties pipelineConfig;
	
	/**
	 * 
	 * @param pipelineSpecification
	 * @return
	 */
	public Stream<?> execute(PipelineSpecification pipelineSpecification) {
		if (logger.isInfoEnabled()){
			logger.info("Executing pipeline: " + pipelineSpecification);
		}
		
		this.pipelineConfig = PipelineConfigurationUtils.loadPipelineConfig(pipelineSpecification.getName());
		
		List<Stage> stages = pipelineSpecification.getStages();
		Supplier<?> sourcesSupplier = stages.get(0).getSourceSupplier();
		Object sources = sourcesSupplier.get();
		
		List<Stream<Object>> streams = null;
		if (sources.getClass().isArray()){
			List<Object> s = Arrays.asList((Object[])sources);
			streams = this.buildStreamsFromSources(s);
		}

		Stream<?> currentStream = null;
		for (Stream<Object> sourceStream : streams) {
			currentStream = sourceStream;
			
			for (Stage stage : stages) {
				String stageName = stage.getName();
				if (stageName.equals("computeKeyValues")){
					currentStream = this.processMappingsStage(stage, currentStream);
				} 
				else if (stageName.equals("reduceByKey")){
					currentStream = this.processCombineStage(stage, currentStream);
				} else {
					throw new UnsupportedOperationException("Operator/Stage '" + stageName + "' is not currently supported");
				}
				sourceStream.close();
			}
		}

		return currentStream;
	}
	
	/**
	 * 
	 * @param stage
	 * @param intermediateStream
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <K,V> Stream<Entry<K, V>> processCombineStage(Stage stage, Stream<?> stream) {
		/*
		 * The reason why Function takes Stream<?> is because it could be KeyValue or KeyValues (i.e. Iterator<V>)
		 * So it could be KeyValueStreamProcessingFunction<Stream<Entry<K,V>>, Stream<Entry<?,?>>> or 
		 * KeyValuesAggregatingStreamProcessingFunction<Stream<Entry<K,Iterator<V>>>, Stream<Entry<?,?>>>
		 */
		Function<Stream<?>, Stream<Entry<K, V>>> func = (Function<Stream<?>, Stream<Entry<K, V>>>) stage.getProcessingFunction();
		return func.apply(stream);
	}
	
	/**
	 * 
	 * @param stage
	 * @param stream
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <K,V> Stream<Entry<K, Iterator<V>>> processMappingsStage(Stage stage, Stream<?> stream) {
		Function<Stream<?>, Stream<Entry<K,V>>> func = (Function<Stream<?>, Stream<Entry<K,V>>>) stage.getProcessingFunction();
		MultiValueMap<K, V> map = new MultiValueMap<>();
		func.apply(stream).forEach(entry -> map.addEntry(entry));
		return map.stream();
	}
	
	/**
	 * 
	 * @param sources
	 * @return
	 */
	private List<Stream<Object>> buildStreamsFromSources(List<Object> sources){
		List<Stream<Object>> streams = new ArrayList<>();
		for (Object source : sources) {
			if (source instanceof URI){
				URI uri = (URI) source;
				this.checkIfURISchemeSupported(uri);
				Stream<Object> stream = this.buildStreamFromURI(uri);
				streams.add(stream);
			}
		}
		return streams;
	}
	
	/**
	 * 
	 * @param uri
	 * @return
	 */
	private Stream<Object> buildStreamFromURI(URI uri) {
		String streamBuilderClassName = this.pipelineConfig.getProperty("stream.builder." + uri.getScheme());
		try {
			Object streamBuilderInstance = ReflectionUtils.newDefaultInstance(Class.forName(streamBuilderClassName, true, Thread.currentThread().getContextClassLoader()));
			Method triggerMethod = ReflectionUtils.findMethod(streamBuilderInstance.getClass(), Stream.class, URI.class);	
			triggerMethod.setAccessible(true);		
			return (Stream<Object>) triggerMethod.invoke(streamBuilderInstance, uri);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to execute pipeline", e);
		}
	}

	/**
	 * 
	 * @param sources
	 */
	private void checkIfURISchemeSupported(URI uri) {
		/*
		 * NOTE TO SELF: We can still support other schemes (e.g., HDFS). It will simply be processed locally (good for testing)
		 */
		if (!"file".equals(uri.getScheme())){
			throw new IllegalArgumentException("Unsupported URI scheme: " + uri.getScheme() + " - (" + uri + "). "
					+ "This delegate only supports 'file' scheme (e.g., file://temp/foo.txt) for URI-based sources.");
		}
	}
}

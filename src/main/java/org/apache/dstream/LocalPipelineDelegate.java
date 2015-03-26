package org.apache.dstream;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.PipelineSpecification.Stage;
import org.apache.dstream.SerializableLambdas.Function;
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
	@SuppressWarnings("unchecked")
	public Stream<?> execute(PipelineSpecification pipelineSpecification) {
		if (logger.isInfoEnabled()){
			logger.info("Executing pipeline: " + pipelineSpecification);
		}
		
		this.pipelineConfig = PipelineConfigurationUtils.loadPipelineConfig(pipelineSpecification.getName());
		
		List<Stream<?>> streams = this.buildStreamsFromSources(pipelineSpecification.getSources());
		Object intermediateResult = null;
		Stream<?> currentStream = null;
		for (Stream<?> sourceStream : streams) {
			List<Stage> stages = pipelineSpecification.getStages();
			currentStream = sourceStream;
			
			for (Stage stage : stages) {
				String stageName = stage.getName();
				if (stageName.equals("computeMappings")){
					intermediateResult = this.processMappingsStage(stage, currentStream);
					currentStream = ((List<Entry<?, ?>>)intermediateResult).stream();
				} 
				else if (stageName.equals("combine")){
					intermediateResult = this.processCombineStage(stage, (Stream<Entry<Object, Object>>) currentStream);
					currentStream = ((Map<?, ?>)intermediateResult).entrySet().stream();
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
	private Map<?, ?> processCombineStage(Stage stage, Stream<Entry<Object, Object>> intermediateStream) {
		Object processingInstruction = stage.getProcessingInstruction();
		Map<Object, Object> results = null;
		if (processingInstruction instanceof BinaryOperator<?>){
			@SuppressWarnings("unchecked")
			BinaryOperator<Object> func = (BinaryOperator<Object>) processingInstruction;		
			results = intermediateStream.collect(Collectors.toMap(s -> s.getKey(), s -> s.getValue(), func));
		}
		return results;
	}
	
	/**
	 * 
	 * @param stage
	 * @param stream
	 * @return
	 */
	private List<Entry<?, ?>> processMappingsStage(Stage stage, Stream<?> stream) {
		Object processingInstruction = stage.getProcessingInstruction();
		List<Entry<?, ?>> results = null;
		if (processingInstruction instanceof Function){
			@SuppressWarnings("unchecked")
			Function<Stream<?>, Stream<Entry<?,?>>> func = (Function<Stream<?>, Stream<Entry<?,?>>>) processingInstruction;
			results = func.apply(stream).collect(Collectors.toList());
		} else {
			throw new IllegalStateException("Unsupported function type");
		}
		return results;
	}
	
	/**
	 * 
	 * @param sources
	 * @return
	 */
	private List<Stream<?>> buildStreamsFromSources(List<Object> sources){
		List<Stream<?>> streams = new ArrayList<>();
		for (Object source : sources) {
			if (source instanceof URI){
				URI uri = (URI) source;
				this.checkIfURISchemeSupported(uri);
				Stream<?> stream = this.buildStreamFromURI(uri);
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
	private Stream<?> buildStreamFromURI(URI uri) {
		String streamBuilderClassName = this.pipelineConfig.getProperty("stream.builder." + uri.getScheme());
		try {
			Object streamBuilderInstance = ReflectionUtils.newDefaultInstance(Class.forName(streamBuilderClassName, true, Thread.currentThread().getContextClassLoader()));
			Method triggerMethod = ReflectionUtils.findMethod(streamBuilderInstance.getClass(), Stream.class, URI.class);	
			triggerMethod.setAccessible(true);		
			return (Stream<?>) triggerMethod.invoke(streamBuilderInstance, uri);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to execute pipeline", e);
		}
	}

	/**
	 * 
	 * @param sources
	 */
	private void checkIfURISchemeSupported(URI uri) {
		if (!"file".equals(uri.getScheme())){
			throw new IllegalArgumentException("Unsupported URI scheme: " + uri);
		}
	}
}

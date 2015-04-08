package org.apache.dstream.local;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.dstream.PipelineSpecification;
import org.apache.dstream.PipelineSpecification.Stage;
import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.SourceSupplier;
import org.apache.dstream.support.Partitioner;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.dstream.utils.ReflectionUtils;

public class LocalExecutionEngine<T> {
	
	private final Properties pipelineConfig;
	
	private final PipelineSpecification pipelineSpecification;
	
	private final Map<Integer, Map<Integer, ?>> stageResults;
	
	/*
	 * stage 1
	 * 	partition 1
	 * 	partition 2
	 * stage 2
	 * 	partition 1
	 * 	partition 2
	 * 	
	 */
	
	
	public LocalExecutionEngine(PipelineSpecification pipelineSpecification){
		Assert.notNull(pipelineSpecification, "'pipelineSpecification' must not be null");
		this.pipelineConfig = PipelineConfigurationUtils.loadPipelineConfig(pipelineSpecification.getName());
		this.pipelineSpecification = pipelineSpecification;
		this.stageResults = new ConcurrentHashMap<>();
	}
	
	public Stream<T>[] execute() {
		List<Stream<Object>> stageInputStreams = null;
		List<Stage> stages = pipelineSpecification.getStages();
		for (Stage stage : stages) {
			Map<Integer, ?> stagePartitions = this.stageResults.get(stage.getId());
			if (stagePartitions == null){
				stagePartitions = new ConcurrentHashMap<>();
				this.stageResults.put(stage.getId(), stagePartitions);
				
				if (stage.getId() == 0){
					SourceSupplier<?> sourceSupplier = stage.getSourceSupplier();
					Assert.notNull(sourceSupplier, "'sourceSupplier' must not be null for initial stage: " + stage);
					stageInputStreams = this.buildStreamsFromSources(sourceSupplier.get()); // initial split; only by the amount of sources; subsequent will be from partitions
				} 
				else {
					stageInputStreams = null;// build from partitions
				}

				for (Stream<?> stageInputStream : stageInputStreams) {
					// can be multi-threaded
					this.processStage(stage, stageInputStream);
				}
			}
		}
		return null;
	}
	
	private void processStage(Stage stage, Stream<?> sourceStream) {	
		if (stage.getName().equals("computeKeyValues")){
			this.processMappingsStage(stage, sourceStream);
		} 
		else {
			throw new UnsupportedOperationException("Stage " + stage.getName() + " is not supported");
		}
	}
	
	/**
	 * Will process stage that produces Key/Value pairs writing the results to partitions
	 * 
	 * @param stage
	 * @param stream
	 */
	private void processMappingsStage(Stage stage, Stream<?> stream) {
		Partitioner partitioner = stage.getPartitioner();
		ConcurrentHashMap<Integer, MultiValueMap<Object, Object>> partitions = (ConcurrentHashMap<Integer, MultiValueMap<Object, Object>>) this.stageResults.get(stage.getId());
		
//		int partitionId = partitioner.getPartition("FOO");
//		MultiValueMap partition = partitions.get(partitionId);
//		partition.addEntry(entry);
		
		
		
		
		Map<Integer, Entry<Object, List<Object>>> stagePartitions = (Map<Integer, Entry<Object, List<Object>>>) this.stageResults.get(stage.getId());
		Function<Stream<?>, Stream<Entry<Object, Object>>> func = (Function<Stream<?>, Stream<Entry<Object, Object>>>) stage.getProcessingFunction();
		MultiValueMap<Object, Object> map = new MultiValueMap<>();
		func.apply(stream).forEach(entry -> {
			int partitionId = partitioner.getPartition(entry.getKey());
//			xMultiValueMap partition = partitions.putIfAbsent(key, value)
		});
//		func.apply(stream).forEach(entry -> map.addEntry(entry));
//		
//		map.streamEntries().forEach(entry -> stagePartitions.merge(1, entry, Utils::mergeEntries));
	}
	
	
	
	private List<Stream<Object>> buildStreamsFromSources(Object[] sources){
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
	
	private List<Stream<Object>> buildStreamsFromPartitions(int parentStageId) {
		Map<Integer, ?> partitions = this.stageResults.get(parentStageId);
		for (Entry<Integer, ?> partition : partitions.entrySet()) {
			int partitionId = partition.getKey();
			
		}
		
		return null;
	}
}

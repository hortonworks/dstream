package org.apache.dstream.tez;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DistributablePipelineSpecification.Stage;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.tez.io.KeyWritable;
import org.apache.dstream.tez.io.ValueWritable;
import org.apache.dstream.tez.utils.HdfsSerializerUtils;
import org.apache.dstream.tez.utils.SequenceFileOutputStreamsBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public class TezExecutableDAGBuilder {
	
	private final Logger logger = LoggerFactory.getLogger(TezExecutableDAGBuilder.class);
	
	private final DAG dag;
	
	private final FileSystem fs;
	
	private final ExecutionContextAwareTezClient tezClient;
	
	private final OrderedPartitionedKVEdgeConfig edgeConf;
	
	private Vertex lastVertex;
	
	// TEZ Properties
	private final Class<?> inputFormatClass;
	
	private final String externallyConfiguredSources;

	
	/**
	 * 
	 * @param pipelineName
	 * @param tezConfiguration
	 */
	public TezExecutableDAGBuilder(String pipelineName, ExecutionContextAwareTezClient tezClient, String externallyConfiguredSources, Class<?> inputFormatClass) {
		this.dag = DAG.create(pipelineName + "_" + System.currentTimeMillis());
		this.tezClient = tezClient;
		try {
			this.fs = FileSystem.get(this.tezClient.getTezConfiguration());
		} catch (Exception e) {
			throw new IllegalStateException("Failed to access FileSystem", e);
		}
		
		this.edgeConf = OrderedPartitionedKVEdgeConfig
				.newBuilder("org.apache.dstream.tez.io.KeyWritable",
						"org.apache.dstream.tez.io.ValueWritable",
						TezDelegatingPartitioner.class.getName(), null).build();
		this.externallyConfiguredSources = externallyConfiguredSources;
		this.inputFormatClass = inputFormatClass;
	}
	
	/**
	 * 
	 * @param stage
	 */
	public void addStage(Stage stage, int parallelizm) {	
		String vertexName = stage.getId() + "_" + stage.getName();
		
		UserPayload payload = this.createPayloadFromTaskSerPath(this.modifyProcessingInstructionIfNecessary(stage), this.dag.getName(), vertexName);

		ProcessorDescriptor pd = ProcessorDescriptor.create(TezTaskProcessor.class.getName()).setUserPayload(payload);
		
		Vertex vertex = this.lastVertex == null 
				? Vertex.create(vertexName, pd) 
						: Vertex.create(vertexName, pd, parallelizm);
		
		vertex.addTaskLocalFiles(this.tezClient.getLocalResources());
		
		if (stage.getId() == 0){	
			SourceSupplier<?> sourceSupplier = stage.getSourceSupplier();
			Object[] sources = sourceSupplier.get();
			
			if (sources == null || sources.length == 0){
				throw new IllegalStateException("External sources configuraration is not currently supported. FIX!");
//				sources = Stream.of(this.externallyConfiguredSources.split(",")).map(s -> {
//					try {
//						return new URI(s);
//					} catch (Exception e) {
//						throw new IllegalStateException("Filed to create URI from " + s);
//					}
//				}).toArray();
			}
			if (sources != null){
				if (sources[0] instanceof URI){
					URI[] uris = Arrays.copyOf(sources, sources.length, URI[].class);
					DataSourceDescriptor dataSource = this.buildDataSourceDescriptorFromUris(uris);
					vertex.addDataSource(vertexName + "_INPUT", dataSource);
				} 
				else {
					throw new IllegalArgumentException("Unsupported sources: " + Arrays.asList(stage.getSourceSupplier()));
				}
			}
		}
		
		this.dag.addVertex(vertex);
		
		if (this.lastVertex != null){
			Edge edge = Edge.create(this.lastVertex, vertex, this.edgeConf.createDefaultEdgeProperty());
			this.dag.addEdge(edge);
		}
		this.lastVertex = vertex;
	}
	
	/**
	 * This method will modify processing instruction if 
	 * - first stage
	 * - source item type is not Entry<K,V> 
	 * 
	 * Also, if source item type is NOT Writable, the modified function will have extractors
	 * 
	 * @param stage
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Function modifyProcessingInstructionIfNecessary(Stage stage) {
		Function processingFunction = stage.getProcessingFunction();

		if (stage.getId() == 0 && !Entry.class.isAssignableFrom(stage.getSourceItemType())){
			Function<Object, Object> func = (Function<Object, Object>) processingFunction;		
			if (Writable.class.isAssignableFrom(stage.getSourceItemType())){
				processingFunction = (Function) func.<Stream<Entry<Object, Object>>>compose(stream -> stream.map(s -> s.getValue()));
			} 
			else {
				
				ParameterizedType parameterizedType = (ParameterizedType) this.inputFormatClass.getGenericSuperclass();
				Type type = parameterizedType.getActualTypeArguments()[1];
				if (Text.class.getName().equals(type.getTypeName())){
					processingFunction = (Function) func.compose((Function<?, ?>)(Stream<Entry<Object, Object>> stream) -> stream.map(s -> s.getValue().toString()));
				} 
				else {
					throw new IllegalStateException("Can't determine modified function");
				}
			}
		}	
		return processingFunction;
	}
	
	/**
	 * 
	 * @return
	 */
	public Callable<Stream<Object>[]> build(){
		String outputPath = this.tezClient.getClientName() + "/" + this.dag.getName() + "/out";
		this.createDataSink(this.lastVertex, 
				this.tezClient.getClientName() + "_OUTPUT", 
				KeyWritable.class, 
				ValueWritable.class, 
				SequenceFileOutputFormat.class, outputPath);
		
		TezDagExecutor<Object> dagExecutor = new TezDagExecutor(this.tezClient, this.dag, new SequenceFileOutputStreamsBuilder(this.fs, outputPath, this.tezClient.getTezConfiguration()));
		
		return dagExecutor;
	}
	
	/**
	 * 
	 * @param sources
	 * @return
	 */
	private DataSourceDescriptor buildDataSourceDescriptorFromUris(URI[] sources) {
		String inputPath = 
				StringUtils.collectionToCommaDelimitedString(Stream.of(sources).map(uri -> uri.getPath()).collect(Collectors.toList()));
		DataSourceDescriptor dataSource = MRInput.createConfigBuilder(this.tezClient.getTezConfiguration(), this.inputFormatClass, inputPath).build();
		return dataSource;
	}
	
	/**
	 * 
	 * @param task
	 * @param pipelineName
	 * @param stageName
	 * @param stageId
	 * @return
	 */
	private UserPayload createPayloadFromTaskSerPath(Object task, String pipelineName, String vertexName){
		org.apache.hadoop.fs.Path mapTaskPath = 
				HdfsSerializerUtils.serialize(task, this.fs, new org.apache.hadoop.fs.Path(pipelineName + "/tasks/" + vertexName + ".ser"));
		UserPayload payload = UserPayload.create(ByteBuffer.wrap(mapTaskPath.toString().getBytes()));
		return payload;
	}
	
	/**
	 * 
	 * @param vertex
	 * @param name
	 * @param keyClass
	 * @param valueClass
	 * @param outputFormatClass
	 * @param outputPath
	 */
	private void createDataSink(Vertex vertex, String name, Class<? extends Writable> keyClass, Class<? extends Writable> valueClass, Class<?> outputFormatClass, String outputPath){
		JobConf dsConfig = this.buildJobConf(keyClass, valueClass);
		DataSinkDescriptor dataSink = MROutput.createConfigBuilder(dsConfig, outputFormatClass, outputPath).build();
		vertex.addDataSink(name, dataSink);
	}
	
	/**
	 * 
	 * @param keyClass
	 * @param valueClass
	 * @return
	 */
	private JobConf buildJobConf(Class<? extends Writable> keyClass, Class<? extends Writable> valueClass){
		JobConf jobConf = new JobConf(this.tezClient.getTezConfiguration());
		jobConf.setOutputKeyClass(keyClass);
		jobConf.setOutputValueClass(valueClass);
		return jobConf;
	}
}

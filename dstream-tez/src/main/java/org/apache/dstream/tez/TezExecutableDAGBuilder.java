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
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.tez.io.KeyWritable;
import org.apache.dstream.tez.io.ValueWritable;
import org.apache.dstream.tez.utils.HdfsSerializerUtils;
import org.apache.dstream.tez.utils.SequenceFileOutputStreamsBuilder;
import org.apache.dstream.utils.Assert;
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
	
	private final ExecutionContextAwareTezClient tezClient;
	
	private final OrderedPartitionedKVEdgeConfig edgeConf;
	
	private Vertex lastVertex;
	
	// TEZ Properties
	private final Class<?> inputFormatClass;
	
	/**
	 * 
	 * @param pipelineName
	 * @param tezConfiguration
	 */
	public TezExecutableDAGBuilder(String pipelineName, ExecutionContextAwareTezClient tezClient, Class<?> inputFormatClass) {
		this.dag = DAG.create(pipelineName + "_" + System.currentTimeMillis());
		this.tezClient = tezClient;
		
		//TODO need to figure out when and why would the Edge e different and how to configure it
		this.edgeConf = OrderedPartitionedKVEdgeConfig
				.newBuilder("org.apache.dstream.tez.io.KeyWritable",
						"org.apache.dstream.tez.io.ValueWritable",
						TezDelegatingPartitioner.class.getName(), null).build();
		this.inputFormatClass = inputFormatClass;
	}
	
	/**
	 * 
	 * @param stage
	 */
	public void addStage(Stage stage, int parallelizm) {	
		String vertexName = stage.getId() + "_" + stage.getName();
		
		UserPayload payload = this.createPayloadFromTaskSerPath(this.composeFunctionIfNecessary(stage), this.dag.getName(), vertexName);

		ProcessorDescriptor pd = ProcessorDescriptor.create(TezTaskProcessor.class.getName()).setUserPayload(payload);
		
		Vertex vertex = this.lastVertex == null 
				? Vertex.create(vertexName, pd) 
						: Vertex.create(vertexName, pd, parallelizm);
		
		vertex.addTaskLocalFiles(this.tezClient.getLocalResources());
		
		if (stage.getId() == 0){	
			SourceSupplier<?> sourceSupplier = stage.getSourceSupplier();
			Object[] sources = sourceSupplier.get();
			
			Assert.notEmpty(sources, "'sources' must not be null or empty");
			
			//TODO add support for non URI-based sources (e.g., Collections)
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
		if (logger.isDebugEnabled()){
			logger.debug("Created Vertex: " + vertex);
		}
		this.lastVertex = vertex;
	}
	
	/**
	 * This method will modify processing instruction to accommodate Tez's KV Reader
	 * for cases where Stream type is non-Entry (e.g., String). It will compose a new Function
	 * with value extracting mapper to comply with user defined types including extraction of 
	 * value from Writable.
	 * It also supports dealing with Writable directly if Writable is the type of the Stream.
	 * Function will be composed if
	 * - first stage
	 * - source item type is not Entry<K,V> 
	 * 
	 * @param stage
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Function<Stream<?>, Stream<?>> composeFunctionIfNecessary(Stage stage) {
		Function<Stream<?>, Stream<?>> processingFunction = stage.getProcessingFunction();

		if (stage.getId() == 0 && !Entry.class.isAssignableFrom(stage.getSourceItemType())){	
			if (Writable.class.isAssignableFrom(stage.getSourceItemType())){
				processingFunction = processingFunction.compose((Function<Stream<?>, Stream<?>>)stream -> stream.map(s -> ((Entry<?,?>)s).getValue()));
			} 
			else {
				ParameterizedType parameterizedType = (ParameterizedType) this.inputFormatClass.getGenericSuperclass();
				Type type = parameterizedType.getActualTypeArguments()[1];
				if (Text.class.getName().equals(type.getTypeName())){
					processingFunction = processingFunction.compose((Function<Stream<?>, Stream<?>>)stream -> ((Stream<Entry<?, ?>>)stream).map(s -> s.getValue().toString()));
				} 
				else {
					//TODO need to design some type of extensible converter to support multiple types of Writable
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Callable<Stream<Object>[]> build(){
		// TODO add support for externally configurable output location
		String outputPath = this.tezClient.getClientName() + "/" + this.dag.getName() + "/out";
		this.createDataSink(this.lastVertex, 
				this.tezClient.getClientName() + "_OUTPUT", 
				KeyWritable.class, 
				ValueWritable.class, 
				SequenceFileOutputFormat.class, outputPath);
		
		TezDagExecutor<Object> dagExecutor = new TezDagExecutor(this.tezClient, this.dag, 
				new SequenceFileOutputStreamsBuilder(this.tezClient.getFileSystem(), outputPath, this.tezClient.getTezConfiguration()));
		
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
				HdfsSerializerUtils.serialize(task, this.tezClient.getFileSystem(), new org.apache.hadoop.fs.Path(pipelineName + "/tasks/" + vertexName + ".ser"));
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

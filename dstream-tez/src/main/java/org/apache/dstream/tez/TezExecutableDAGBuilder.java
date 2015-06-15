package org.apache.dstream.tez;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.ExecutionSpec;
import org.apache.dstream.ExecutionSpec.Stage;
import org.apache.dstream.PredicateJoinFunction;
import org.apache.dstream.support.KeyValuesStreamCombinerFunction;
import org.apache.dstream.support.KeyValuesStreamGrouperFunction;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.tez.io.KeyWritable;
import org.apache.dstream.tez.io.ValueWritable;
import org.apache.dstream.tez.utils.HdfsSerializerUtils;
import org.apache.dstream.utils.Assert;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

/**
 * 
 */
public class TezExecutableDAGBuilder {
	
	private final Logger logger = LoggerFactory.getLogger(TezExecutableDAGBuilder.class);
	
	private final DAG dag;
	
	private final ExecutionContextAwareTezClient tezClient;
	
	private final OrderedPartitionedKVEdgeConfig edgeConf;
	
	private final TezDagExecutor dagExecutor;
	
	private Vertex lastVertex;
	
	private int inputOrderCounter;
	
	/**
	 * 
	 * @param pipelineName
	 * @param tezClient
	 * @param inputFormatClass
	 */
	public TezExecutableDAGBuilder(String pipelineName, ExecutionContextAwareTezClient tezClient, Properties pipelineConfig) {
		this.dag = DAG.create(pipelineName + "_" + System.currentTimeMillis());
		this.tezClient = tezClient;
		
		//TODO need to figure out when and why would the Edge be different and how to configure it
		this.edgeConf = OrderedPartitionedKVEdgeConfig
				.newBuilder("org.apache.dstream.tez.io.KeyWritable",
						"org.apache.dstream.tez.io.ValueWritable",
						TezDelegatingPartitioner.class.getName(), null).build();
		this.dagExecutor = new TezDagExecutor(this.tezClient, this.dag);
	}
	
	/**
	 * 
	 * @param stage
	 * @param parallelizm
	 */
	public void addStage(Stage stage) {	
		String vertexName = stage.getName();
		Class<?> inputFormatClass = stage.getId() == 0 ? this.determineInputFormatClass(stage) : null;
		UserPayload payload = this.createPayloadFromTaskSerPath(this.buildTask(stage, inputFormatClass), this.dag.getName(), stage);
		ProcessorDescriptor pd = ProcessorDescriptor.create(TezTaskProcessor.class.getName()).setUserPayload(payload);	
		
		// inputOrderCounter needed to maintain the order of inputs for joins
		Vertex vertex = stage.getId() == 0 
				? Vertex.create(this.inputOrderCounter++ + ":" + vertexName, pd) 
						: Vertex.create(this.inputOrderCounter++ + ":" + vertexName, pd, 
								stage.getParallelizer() == null ? 1 : stage.getParallelizer().getPartitionSize());
				
		vertex.addTaskLocalFiles(this.tezClient.getLocalResources());
		
		this.dag.addVertex(vertex);
		
		if (stage.getId() == 0){	
			SourceSupplier<?> sourceSupplier = stage.getSourceSupplier();
			Object[] sources = sourceSupplier.get();
			
			Assert.notEmpty(sources, "'sources' must not be null or empty");
			
			if (sources != null){
				if (sources[0] instanceof URI){
					URI[] uris = Arrays.copyOf(sources, sources.length, URI[].class);
					DataSourceDescriptor dataSource = this.buildDataSourceDescriptorFromUris(inputFormatClass, uris);
					vertex.addDataSource(this.inputOrderCounter++ + ":" + vertexName + "_INPUT", dataSource);
				} 
				else {
					throw new IllegalArgumentException("Unsupported sources: " + Arrays.asList(stage.getSourceSupplier()));
				}
			}
		} 
		else {
			Edge edge = Edge.create(this.lastVertex, vertex, this.edgeConf.createDefaultEdgeProperty());
			this.dag.addEdge(edge);
		}
		
		if (stage.getDependentExecutionSpec() != null){
			ExecutionSpec execSpec = stage.getDependentExecutionSpec();
			List<Stage> dependentStages = execSpec.getStages();
			
			dependentStages.forEach(dependentStage -> this.addStage(dependentStage));
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private TaskPayload buildTask(Stage stage, Class<?> inputFormatClass) {
		Function<Stream<?>, Stream<?>> processingFunction = (Function<Stream<?>, Stream<?>>) stage.getProcessingFunction();
		if (stage.getAggregatorOperator() != null) {
			Function<Stream<?>,Stream<?>> aggregatingFunction = stage.getOperationNames()[0].equals("group")
					? new KeyValuesStreamGrouperFunction(stage.getAggregatorOperator())
						: new KeyValuesStreamCombinerFunction(stage.getAggregatorOperator());
			if (processingFunction instanceof PredicateJoinFunction){
				((PredicateJoinFunction)processingFunction).composeIntoHash(new KeyValuesStreamCombinerFunction(null));
				((PredicateJoinFunction)processingFunction).composeIntoProbe(aggregatingFunction);
			}
			else {
				processingFunction = processingFunction == null ? aggregatingFunction : processingFunction.compose(aggregatingFunction);
			}
		} 
		else if (processingFunction == null) {
			throw new IllegalStateException("Both processing function and aggregator op are null. "
					+ "This condition is invalid as it will result in a stage with no processing instruction and is definitely a bug. Please report!");
		}
		
		if (stage.getId() == 0 && !Entry.class.isAssignableFrom(stage.getSourceItemType())){	
			if (Writable.class.isAssignableFrom(stage.getSourceItemType())){
				processingFunction = processingFunction.compose(stream -> stream.map(s -> ((Entry)s).getValue()));
			} 
			else {
				ParameterizedType parameterizedType = (ParameterizedType) inputFormatClass.getGenericSuperclass();
				Type type = parameterizedType.getActualTypeArguments()[1];
				if (Text.class.getName().equals(type.getTypeName())){
					processingFunction = processingFunction.compose(stream -> stream.map( s -> ((Entry)s).getValue().toString()));
				} 
				else {
					//TODO need to design some type of extensible converter to support multiple types of Writable
					throw new IllegalStateException("Can't determine modified function");
				}
			}
		}	
		TaskPayload payload = new TaskPayload(processingFunction);
		payload.setParallelizer(stage.getParallelizer());
		return payload;
	}
	
	/**
	 * 
	 */
	public void addDataSink(String outputPath){
		this.createDataSink(this.lastVertex, 
				this.tezClient.getClientName() + "_OUTPUT", 
				KeyWritable.class, 
				ValueWritable.class, 
				SequenceFileOutputFormat.class, outputPath);
		
		this.lastVertex = null;
	}
	
	/**
	 * 
	 * @return
	 */
	public Runnable build(){
		return this.dagExecutor;
	}
	
	/**
	 * 
	 */
	private DataSourceDescriptor buildDataSourceDescriptorFromUris(Class<?> inputFormatClass, URI[] sources) {
		String inputPath = 
				StringUtils.collectionToCommaDelimitedString(Stream.of(sources).map(uri -> uri.getPath()).collect(Collectors.toList()));
		DataSourceDescriptor dataSource = MRInput.createConfigBuilder(this.tezClient.getTezConfiguration(), inputFormatClass, inputPath).groupSplits(false).build();
		return dataSource;
	}
	
	/**
	 * 
	 */
	private UserPayload createPayloadFromTaskSerPath(Object task, String pipelineName, Stage stage){
		org.apache.hadoop.fs.Path mapTaskPath = 
				HdfsSerializerUtils.serialize(task, this.tezClient.getFileSystem(), 
						new org.apache.hadoop.fs.Path(pipelineName + "/tasks/" + stage.getName() + ".ser"));
		UserPayload payload = UserPayload.create(ByteBuffer.wrap(mapTaskPath.toString().getBytes()));
		return payload;
	}
	
	/**
	 * 
	 */
	private void createDataSink(Vertex vertex, String name, Class<? extends Writable> keyClass, Class<? extends Writable> valueClass, 
				Class<?> outputFormatClass, String outputPath){
		JobConf dsConfig = this.buildJobConf(keyClass, valueClass);
		DataSinkDescriptor dataSink = MROutput.createConfigBuilder(dsConfig, outputFormatClass, outputPath).build();
		vertex.addDataSink(name, dataSink);
	}
	
	/**
	 * 
	 */
	private JobConf buildJobConf(Class<? extends Writable> keyClass, Class<? extends Writable> valueClass){
		JobConf jobConf = new JobConf(this.tezClient.getTezConfiguration());
		jobConf.setOutputKeyClass(keyClass);
		jobConf.setOutputValueClass(valueClass);
		return jobConf;
	}
	
	/**
	 * 
	 */
	private Class<?> determineInputFormatClass(Stage firstStage){
		SourceSupplier<?> sourceSupplier = firstStage.getSourceSupplier();
		
		if (sourceSupplier.get()[0] instanceof URI){
			if (firstStage.getSourceItemType().isAssignableFrom(String.class)){
				return TextInputFormat.class;
			} 
			else {
				// TODO design a configurable component to handle other standard and custom input types
				throw new IllegalArgumentException("Failed to determine Input Format class for source item type " + firstStage.getSourceItemType());
			}
		} 
		else {
			throw new IllegalArgumentException("Non URI sources are not supported yet");
		}
	}
}

package org.apache.dstream.tez;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Supplier;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.tez.io.KeyWritable;
import org.apache.dstream.tez.io.ValueWritable;
import org.apache.dstream.tez.utils.HdfsSerializerUtils;
import org.apache.dstream.utils.Assert;
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
public class TezDAGBuilder {
	
	private final Logger logger = LoggerFactory.getLogger(TezDAGBuilder.class);
	
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
	public TezDAGBuilder(String executionName, ExecutionContextAwareTezClient tezClient, Properties executionConfig) {
		this.dag = DAG.create(executionName + "_" + System.currentTimeMillis());
		this.tezClient = tezClient;
		
		//TODO need to figure out when and why would the Edge be different and how to configure it
		this.edgeConf = OrderedPartitionedKVEdgeConfig
				.newBuilder("org.apache.dstream.tez.io.KeyWritable",
						"org.apache.dstream.tez.io.ValueWritable",
						TezDelegatingPartitioner.class.getName(), null).build();
//		edgeConf.cr
		this.dagExecutor = new TezDagExecutor(this.tezClient, this.dag);
	}
	
	/**
	 * 
	 * @param stage
	 * @param parallelizm
	 */
	public void addTask(TaskDescriptor taskDescriptor) {	
		String vertexName = taskDescriptor.getName();
		if (taskDescriptor.getId() == 0){
			this.determineInputFormatClass(taskDescriptor);
		}
		UserPayload payload = this.createPayloadFromTaskSerPath(Task.build(taskDescriptor), this.dag.getName());
		ProcessorDescriptor pd = ProcessorDescriptor.create(TezTaskProcessor.class.getName()).setUserPayload(payload);	
		
		// inputOrderCounter needed to maintain the order of inputs for joins
		Vertex vertex = taskDescriptor.getId() == 0 
				? Vertex.create(this.inputOrderCounter++ + ":" + vertexName, pd) 
						: Vertex.create(this.inputOrderCounter++ + ":" + vertexName, pd, 
								taskDescriptor.getPartitioner() == null ? 1 : taskDescriptor.getPartitioner().getPartitionSize());
				
		vertex.addTaskLocalFiles(this.tezClient.getLocalResources());
		
		this.dag.addVertex(vertex);
		
		if (taskDescriptor.getId() == 0){	
			Supplier<?> sourceSupplier = taskDescriptor.getSourceSupplier();
			
			if (sourceSupplier instanceof SourceSupplier){
				Object[] sources = ((SourceSupplier<?>)sourceSupplier).get();
				
				Assert.notEmpty(sources, "'sources' must not be null or empty");	
				if (sources != null){
					if (sources[0] instanceof URI){
						URI[] uris = Arrays.copyOf(sources, sources.length, URI[].class);
						DataSourceDescriptor dataSource = this.buildDataSourceDescriptorFromUris(taskDescriptor.getInputFormatClass(), uris);
						vertex.addDataSource(this.inputOrderCounter++ + ":" + vertexName + "_INPUT", dataSource);
					} 
					else {
						throw new IllegalArgumentException("Unsupported sources: " + Arrays.asList(taskDescriptor.getSourceSupplier()));
					}
				}
			} 
			else {
				throw new IllegalArgumentException("Urecognized source supplier: " + sourceSupplier);
			}	
		} 
		else {
			Edge edge = Edge.create(this.lastVertex, vertex, this.edgeConf.createDefaultEdgeProperty());
			this.dag.addEdge(edge);
		}
		
		if (taskDescriptor.getDependentTasksChain() != null){
			List<TaskDescriptor> dependentTasks = taskDescriptor.getDependentTasksChain();
			dependentTasks.forEach(this::addTask);
			Edge edge = Edge.create(this.lastVertex, vertex, this.edgeConf.createDefaultEdgeProperty());
			this.dag.addEdge(edge);
		}
		
		if (logger.isDebugEnabled()){
			logger.debug("Created Vertex: " + vertex);
		}
		this.lastVertex = vertex;
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
	private UserPayload createPayloadFromTaskSerPath(Task task, String dagName){
		org.apache.hadoop.fs.Path mapTaskPath = 
				HdfsSerializerUtils.serialize(task, this.tezClient.getFileSystem(), 
						new org.apache.hadoop.fs.Path(dagName + "/tasks/" + task.getId() + "_" + task.getName() + ".ser"));
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
	private void determineInputFormatClass(TaskDescriptor firstTask){
		SourceSupplier<?> sourceSupplier = (SourceSupplier<?>) firstTask.getSourceSupplier();
		Class<?> sourceElementType = (Class<?>) firstTask.getSourceElementType();
		if (sourceSupplier.get()[0] instanceof URI){
			if (sourceElementType.isAssignableFrom(String.class)){
				firstTask.setInputFormatClass(TextInputFormat.class);
			} 
			else {
				// TODO design a configurable component to handle other standard and custom input types
				throw new IllegalArgumentException("Failed to determine Input Format class for source item type " + sourceElementType);
			}
		} 
		else {
			throw new IllegalArgumentException("Non URI sources are not supported yet");
		}
	}
}

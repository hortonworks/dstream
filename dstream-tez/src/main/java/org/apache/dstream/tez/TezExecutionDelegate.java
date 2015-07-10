package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.AbstractStreamExecutionDelegate;
import org.apache.dstream.DistributableConstants;
import org.apache.dstream.DistributableStreamToStreamAdapterFunction;
import org.apache.dstream.KeyValueMappingFunction;
import org.apache.dstream.OperationContext;
import org.apache.dstream.support.Aggregators;
import org.apache.dstream.support.KeyValuesStreamCombinerFunction;
import org.apache.dstream.support.KeyValuesStreamGrouperFunction;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.tez.utils.HadoopUtils;
import org.apache.dstream.tez.utils.SequenceFileOutputStreamsBuilder;
import org.apache.dstream.utils.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.TezConfiguration;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

public class TezExecutionDelegate extends AbstractStreamExecutionDelegate<List<MethodInvocation>> {
	
	private final List<List<TaskDescriptor>> taskChains;
	
	private Properties executionConfig;
	
	private Class<?> sourceElementType ;
	
	private String sourceIdentifier;
	
	private String executionName;
	
	private int sequenceCounter;
	
	private ExecutionContextAwareTezClient tezClient;
	
	
	public TezExecutionDelegate(){
		this.taskChains = new ArrayList<>();
	}

	@Override
	public Runnable getCloseHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Stream<Stream<?>> doExecute(String executionName, Properties executionConfig, OperationContext<List<MethodInvocation>>... operationContexts) {
		this.executionConfig = executionConfig;
		this.executionName = executionName;
		
		for (OperationContext<List<MethodInvocation>> operationContext : operationContexts) {
			List<MethodInvocation> invocations = operationContext.get();		
			for (MethodInvocation invocation : invocations) {
				if (this.sourceElementType == null){
					this.sourceElementType = (Class<?>) ((ReflectiveMethodInvocation)invocation).getUserAttribute("sourceElementType");
					this.sourceIdentifier = (String) ((ReflectiveMethodInvocation)invocation).getUserAttribute("sourceIdentifier");
				}
				String operationName = invocation.getMethod().getName();		
				TaskDescriptor currentTask = this.getCurrentTask();
				if (currentTask.getName() == null){
					currentTask.setName(this.sourceIdentifier);
				}
				
				// create sources
				if (currentTask.getId() == 0){
					String sourceProperty = executionConfig.getProperty(DistributableConstants.SOURCE + this.sourceIdentifier);
					Assert.notEmpty(sourceProperty, DistributableConstants.SOURCE + this.sourceIdentifier +  "' property can not be found in " + 
							this.executionName + ".cfg configuration file.");
					SourceSupplier<?> sourceSupplier = SourceSupplier.create(sourceProperty, null);
					currentTask.setSourceSupplier(sourceSupplier);
					currentTask.setSourceElementType(this.sourceElementType);
				}
				
				if (this.isIntermediateOperation(operationName)){
					this.processIntermediateOperation(invocation);
				}
				else if (this.isShuffleOperation(operationName)){
					this.processShuffleOperation(invocation);
				}
			}
			this.sourceElementType = null;
			this.sourceIdentifier = null;
		}
		
		TezConfiguration tezConfiguration = new TezConfiguration(new Configuration());
		FileSystem fs = HadoopUtils.getFileSystem(tezConfiguration);
		
		if (this.tezClient == null){
			this.createAndTezClient(executionName, fs, tezConfiguration);
		}
		
		TezDAGBuilder dagBuilder = new TezDAGBuilder(executionName, this.tezClient, executionConfig);
		List<String> outputURIs  = new ArrayList<String>();
		
		String output = (String) executionConfig.getOrDefault(DistributableConstants.OUTPUT, this.tezClient.getClientName() + "/out/");
		for (int i = 0; i < this.taskChains.size(); i++) {
			List<TaskDescriptor> taskChain = this.taskChains.get(i);
			taskChain.forEach(task -> dagBuilder.addTask(task));
			output += (this.taskChains.size() > 1 ? "/" + i : "");
			dagBuilder.addDataSink(output);
			outputURIs.add(output);
		}
		
		Runnable executable = dagBuilder.build();
		
		try {
			executable.run();
			Stream<Stream<?>>[] resultStreams = outputURIs.stream().map(uri -> {
				SequenceFileOutputStreamsBuilder<?> ob = new SequenceFileOutputStreamsBuilder<>(this.tezClient.getFileSystem(), uri, this.tezClient.getTezConfiguration());
				return Stream.of(ob.build());
			}).collect(Collectors.toList()).toArray(new Stream[]{});
			
			if (resultStreams.length == 1){
				return resultStreams[0];
			}
			else {
				return Stream.of(resultStreams);
			}
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to execute DAG for " + executionName, e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	//TODO needs some integration with extarnal conf to get credentials
	protected Credentials getCredentials(){
		return null;
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void processIntermediateOperation(MethodInvocation invocation){	
		Function<Stream<?>, Stream<?>> function = invocation.getMethod().getName().equals("compute") 
				? (Function<Stream<?>, Stream<?>>) invocation.getArguments()[0]
						: new DistributableStreamToStreamAdapterFunction(invocation.getMethod().getName(), invocation.getArguments()[0]);

		TaskDescriptor task = this.getCurrentTask();
		task.andThen(function);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void processShuffleOperation(MethodInvocation invocation){
		String operationName = invocation.getMethod().getName();
		if (operationName.equals("reduceGroups")) {
			Object[] arguments = invocation.getArguments();
			TaskDescriptor currentTask = this.getCurrentTask();
			String propertyName = DistributableConstants.MAP_SIDE_COMBINE + currentTask.getId() + "_" + currentTask.getName();
			boolean mapSideCombine = Boolean.parseBoolean((String)this.executionConfig.getOrDefault(propertyName, "false"));

			currentTask.andThen(new KeyValueMappingFunction((Function)arguments[0], (Function)arguments[1], mapSideCombine ? (BinaryOperator)arguments[2] : null));
			
			this.getCurrentTaskChain().add(new TaskDescriptor(this.sequenceCounter++, this.sourceIdentifier));
			TaskDescriptor curresntTaskDescriptor = this.getCurrentTask();
			curresntTaskDescriptor.compose(new KeyValuesStreamCombinerFunction((BinaryOperator)arguments[2]));
		}
		else if (operationName.equals("group")) {
			Object[] arguments = invocation.getArguments();
			TaskDescriptor currentTask = this.getCurrentTask();
			if (arguments.length == 2){
				currentTask.andThen(new KeyValueMappingFunction((Function)arguments[0], (Function)arguments[1], null));
			}
			else {
				currentTask.andThen(new KeyValueMappingFunction((Function)arguments[0], s -> s, null));
			}
			this.getCurrentTaskChain().add(new TaskDescriptor(this.sequenceCounter++, this.sourceIdentifier));
			TaskDescriptor curresntTaskDescriptor = this.getCurrentTask();
			curresntTaskDescriptor.compose(new KeyValuesStreamGrouperFunction(Aggregators::aggregateFlatten));
		}
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' temporarily is not supported");
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private TaskDescriptor getCurrentTask(){
		List<TaskDescriptor> taskChain = this.getCurrentTaskChain();
		if (taskChain.size() == 0){
			taskChain.add(new TaskDescriptor(this.sequenceCounter++));
		}
		return taskChain.get(taskChain.size() - 1);
	}
	
	private List<TaskDescriptor> getCurrentTaskChain(){
		if (this.taskChains.size() == 0){
			this.taskChains.add(new ArrayList<TaskDescriptor>());
		}
		return this.taskChains.get(this.taskChains.size() - 1);
	}
	
	/**
	 * 
	 * @param pipelineSpecification
	 */
	private void createAndTezClient(String executionName, FileSystem fs, TezConfiguration tezConfiguration){	
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, executionName + 
												"/" + TezConstants.CLASSPATH_PATH);
		this.tezClient = new ExecutionContextAwareTezClient(executionName, 
											   tezConfiguration, 
											   localResources, 
											   this.getCredentials(),
											   fs);
		try {
			this.tezClient.start();
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to start TezClient", e);
		}
	}
}

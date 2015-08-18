package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import dstream.DStreamConstants;
import dstream.StreamOperation;
import dstream.StreamOperations;
import dstream.function.DStreamToStreamAdapterFunction;
import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.support.SourceSupplier;
import dstream.utils.Assert;

/**
 * 
 */
class TaskDescriptorChainBuilder {
	
	private final List<TaskDescriptor> taskChain;
	
	private final StreamOperations operationsGroup;
	
	private final String executionName;
	
	private final Properties executionConfig;
	
	private int sequenceIdCounter;
	
	/**
	 * 
	 * @param executionName
	 * @param invocationChain
	 * @param executionConfig
	 */
	public TaskDescriptorChainBuilder(String executionName, StreamOperations operationsGroup, Properties executionConfig){
		this.taskChain = new ArrayList<>();
		this.operationsGroup = operationsGroup;
		this.executionName = executionName;
		this.executionConfig = executionConfig;
	}

	/**
	 * 
	 * @return
	 */
	public List<TaskDescriptor> build(){
		List<StreamOperation> streamOperations = this.operationsGroup.getOperations();

		for (StreamOperation streamOperation : streamOperations) {	
			TaskDescriptor taskDescriptor;
			if (streamOperation.getLastOperationName().equals("join") || streamOperation.getLastOperationName().startsWith("union")){
				String name = this.taskChain.get(this.taskChain.size() - 1).getOperationName();
				if (!name.contains("classify")){
					throw new IllegalStateException("Unclassified stream combines (join/union) are not supported at the moment by Tez.");
				}
			}
			if (!streamOperation.getDependentStreamOperations().isEmpty()){
				taskDescriptor = this.createTaskDescriptorForStreamCombineOperations(streamOperation);
			}
			else {
				taskDescriptor = this.createTaskDescriptor(streamOperation.getLastOperationName());
			}
			
			taskDescriptor.andThen(streamOperation.getStreamOperationFunction());
			this.taskChain.add(taskDescriptor);
		}
		return this.taskChain;
	}
	
	/**
	 * Creates {@link TaskDescriptor} for stream combine operations (i.e., join, union, unionAll)
	 */
	private TaskDescriptor createTaskDescriptorForStreamCombineOperations(StreamOperation streamOperation){

		TaskDescriptor taskDescriptor = this.createTaskDescriptor(streamOperation.getLastOperationName());
		for (StreamOperations dependentOps : streamOperation.getDependentStreamOperations()) {
			TaskDescriptorChainBuilder builder = new TaskDescriptorChainBuilder(executionName, dependentOps, executionConfig);
			List<TaskDescriptor> dependentDescriptors = builder.build();
			taskDescriptor.addDependentTasksChain(dependentDescriptors);
		}
		return taskDescriptor;
	}
	
	/**
	 * 
	 */
	private void createAndAddInitialTaskDescriptor(){
		TaskDescriptor td = this.createTaskDescriptor("map");
		SerFunction<Stream<?>, Stream<?>> function = new DStreamToStreamAdapterFunction("map", (SerFunction<?,?>)s -> s);
		td.andThen(function);
		this.taskChain.add(td);
	}
	/**
	 * 
	 * @param td
	 */
	private void initializeTaskInputsIfNecessary(TaskDescriptor td){
		if (td.getId() == 0 && td.getSourceSupplier() == null){
			String sourceProperty = executionConfig.getProperty(DStreamConstants.SOURCE + this.operationsGroup.getPipelineName());
			Assert.notEmpty(sourceProperty, DStreamConstants.SOURCE + this.operationsGroup.getPipelineName() +  "' property can not be found in " + 
					this.executionName + ".cfg configuration file.");
			SourceSupplier<?> sourceSupplier = SourceSupplier.create(sourceProperty, null);
			td.setSourceSupplier(sourceSupplier);
			td.setSourceElementType(this.operationsGroup.getSourceElementType());
		}
	}
	
	/**
	 * 
	 */
	private TaskDescriptor getCurrentTask(){
		if (this.taskChain.size() != 0){
			return this.taskChain.get(this.taskChain.size() - 1);
		}
		return null;
	}

	/**
	 * 
	 */
	private TaskDescriptor createTaskDescriptor(String operationName){
		TaskDescriptor taskDescriptor = new TaskDescriptor(this.sequenceIdCounter++, 
				this.operationsGroup.getPipelineName(), operationName, this.executionConfig, this.getCurrentTask());
		this.initializeTaskInputsIfNecessary(taskDescriptor);
		return taskDescriptor;
	}
}
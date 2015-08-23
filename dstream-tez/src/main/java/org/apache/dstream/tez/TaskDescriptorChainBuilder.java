package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import dstream.DStreamConstants;
import dstream.DStreamOperation;
import dstream.DStreamOperations;
import dstream.support.SourceSupplier;
import dstream.utils.Assert;

/**
 * 
 */
class TaskDescriptorChainBuilder {
	
	private final List<TaskDescriptor> taskChain;
	
	private final DStreamOperations executionPipeline;
	
	private final String executionName;
	
	private final Properties executionConfig;
	
	private int sequenceIdCounter;
	
	/**
	 * 
	 * @param executionName
	 * @param invocationChain
	 * @param executionConfig
	 */
	public TaskDescriptorChainBuilder(String executionName, DStreamOperations executionPipeline, Properties executionConfig){
		this.taskChain = new ArrayList<>();
		this.executionPipeline = executionPipeline;
		this.executionName = executionName;
		this.executionConfig = executionConfig;
	}

	/**
	 * 
	 * @return
	 */
	public List<TaskDescriptor> build(){
		List<DStreamOperation> streamOperations = this.executionPipeline.getOperations();

		for (DStreamOperation streamOperation : streamOperations) {	
			TaskDescriptor taskDescriptor;
			if (streamOperation.getLastOperationName().equals("join") || streamOperation.getLastOperationName().startsWith("union")){
				String name = this.taskChain.get(this.taskChain.size() - 1).getOperationName();
				if (!name.contains("classify")){
					throw new IllegalStateException("Unclassified stream combines (join/union) are not supported at the moment by Tez.");
				}
			}
			if (!streamOperation.getCombinableStreamOperations().isEmpty()){
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
	private TaskDescriptor createTaskDescriptorForStreamCombineOperations(DStreamOperation streamOperation){

		TaskDescriptor taskDescriptor = this.createTaskDescriptor(streamOperation.getLastOperationName());
		for (DStreamOperations dependentOps : streamOperation.getCombinableStreamOperations()) {
			TaskDescriptorChainBuilder builder = new TaskDescriptorChainBuilder(executionName, dependentOps, executionConfig);
			List<TaskDescriptor> dependentDescriptors = builder.build();
			taskDescriptor.addDependentTasksChain(dependentDescriptors);
		}
		return taskDescriptor;
	}
	
	/**
	 * 
	 * @param td
	 */
	private void initializeTaskInputsIfNecessary(TaskDescriptor td){
		if (td.getId() == 0 && td.getSourceSupplier() == null){
			String sourceProperty = executionConfig.getProperty(DStreamConstants.SOURCE + this.executionPipeline.getName());
			Assert.notEmpty(sourceProperty, DStreamConstants.SOURCE + this.executionPipeline.getName() +  "' property can not be found in " + 
					this.executionName + ".cfg configuration file.");
			SourceSupplier<?> sourceSupplier = SourceSupplier.create(sourceProperty, null);
			td.setSourceSupplier(sourceSupplier);
			td.setSourceElementType(this.executionPipeline.getSourceElementType());
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
				this.executionPipeline.getName(), operationName, this.executionConfig, this.getCurrentTask());
		this.initializeTaskInputsIfNecessary(taskDescriptor);
		return taskDescriptor;
	}
}
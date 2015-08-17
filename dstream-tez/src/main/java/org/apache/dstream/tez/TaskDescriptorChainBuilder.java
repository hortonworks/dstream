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
import dstream.utils.KeyValuesNormalizer;

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
	@SuppressWarnings("unchecked")
	public List<TaskDescriptor> build(){
		List<StreamOperation> streamOperations = this.operationsGroup.getOperations();
		
//		if (streamOperations.size() == 0){
//			this.createAndAddInitialTaskDescriptor();
//		}
//		else {
			for (StreamOperation streamOperation : streamOperations) {	
				TaskDescriptor taskDescriptor;
				if (!streamOperation.getDependentStreamOperations().isEmpty()){
					taskDescriptor = this.createTaskDescriptorForStreamCombineOperations(streamOperation);
				}
				else {
					taskDescriptor = this.createTaskDescriptor(streamOperation.getLastOperationName());
				}
				
				taskDescriptor.andThen(streamOperation.getStreamOperationFunction());
				this.taskChain.add(taskDescriptor);
				
//				if (streamOperation.getGroupClassifier() != null) {
//					if (this.taskChain.size() == 1){
//						taskDescriptor = this.createTaskDescriptor("group");
//						this.taskChain.add(taskDescriptor);
//						taskDescriptor.andThen(stream -> KeyValuesNormalizer.normalizeStream((Stream<Entry<Object, Iterator<Object>>>) stream));
//						taskDescriptor = this.getCurrentTask().getPreviousTaskDescriptor();
//					}
//					taskDescriptor.getGrouper().setClassifier((SerFunction<Object, ?>) streamOperation.getGroupClassifier());
//				}
			}
//		}
		
		return this.taskChain;
	}
	
	/**
	 * Creates {@link TaskDescriptor} for stream combine operations (i.e., join, union, unionAll)
	 */
	@SuppressWarnings("unchecked")
	private TaskDescriptor createTaskDescriptorForStreamCombineOperations(StreamOperation streamOperation){
//		if (this.taskChain.size() == 0){
//			this.createAndAddInitialTaskDescriptor();
//		}
		TaskDescriptor taskDescriptor = this.createTaskDescriptor(streamOperation.getLastOperationName());
		for (StreamOperations dependentOps : streamOperation.getDependentStreamOperations()) {
			TaskDescriptorChainBuilder builder = new TaskDescriptorChainBuilder(executionName, dependentOps, executionConfig);
			List<TaskDescriptor> dependentDescriptors = builder.build();
			taskDescriptor.addDependentTasksChain(dependentDescriptors);
		}
//		SerFunction<Stream<?>, Stream<?>> normailizer = null;//KeyValuesNormalizer::normalizeStream;
//		@SuppressWarnings("rawtypes")
//		SerFunction typeLessNormalizer = normailizer;
		taskDescriptor.andThen(streamOperation.getStreamOperationFunction().compose(s -> {
			System.out.println();
			return s;}));
//		taskDescriptor.andThen(streamOperation.getStreamOperationFunction().compose(normailizer));
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
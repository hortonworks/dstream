package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.DistributableConstants;
import org.apache.dstream.StreamInvocationChain;
import org.apache.dstream.function.DStreamToStreamAdapterFunction;
import org.apache.dstream.function.KeyValueMappingFunction;
import org.apache.dstream.function.ValuesReducingFunction;
import org.apache.dstream.function.ValuesGroupingFunction;
import org.apache.dstream.function.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.support.Aggregators;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Tuples.Tuple2;

class TaskDescriptorChainBuilder {
	
	private final List<TaskDescriptor> taskChain;
	
	private final StreamInvocationChain invocationChain;
	
	private final String executionName;
	
	private final Properties executionConfig;
	
	private int sequenceIdCounter;
	
//	private MethodInvocation previous
	
	/**
	 * 
	 * @param executionName
	 * @param invocationChain
	 * @param executionConfig
	 */
	public TaskDescriptorChainBuilder(String executionName, StreamInvocationChain invocationChain, Properties executionConfig){
		this.taskChain = new ArrayList<>();
		this.invocationChain = invocationChain;
		this.executionName = executionName;
		this.executionConfig = executionConfig;
	}

	/**
	 * 
	 * @return
	 */
	public List<TaskDescriptor> build(){
		List<MethodInvocation> invocations = this.invocationChain.getInvocations();
		for (MethodInvocation invocation : invocations) {
			String operationName = invocation.getMethod().getName();		
			TaskDescriptor currentTask = this.getCurrentTask(invocation);
			if (currentTask.getName() == null){
				currentTask.setName(this.invocationChain.getSourceIdentifier());
			}
			
			// create sources
			if (currentTask.getId() == 0 && currentTask.getSourceSupplier() == null){
				String sourceProperty = executionConfig.getProperty(DistributableConstants.SOURCE + this.invocationChain.getSourceIdentifier());
				Assert.notEmpty(sourceProperty, DistributableConstants.SOURCE + this.invocationChain.getSourceIdentifier() +  "' property can not be found in " + 
						this.executionName + ".cfg configuration file.");
				SourceSupplier<?> sourceSupplier = SourceSupplier.create(sourceProperty, null);
				currentTask.setSourceSupplier(sourceSupplier);
				currentTask.setSourceElementType(this.invocationChain.getSourceElementType());
			}
			
			if (this.isIntermediateOperation(operationName)){
				this.processIntermediateOperation(invocation);
			}
			else if (this.isShuffleOperation(operationName)){
				this.processShuffleOperation(invocation);
			}
		}
		
		return this.taskChain;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void processShuffleOperation(MethodInvocation invocation){
		String operationName = invocation.getMethod().getName();
		Object[] arguments = invocation.getArguments();
		TaskDescriptor currentTask = this.getCurrentTask(invocation);
		if (operationName.equals("reduceGroups")) {
			String propertyName = DistributableConstants.MAP_SIDE_COMBINE + currentTask.getId() + "_" + currentTask.getName();
			boolean mapSideCombine = Boolean.parseBoolean((String)this.executionConfig.getOrDefault(propertyName, "false"));
			BinaryOperator aggregator = mapSideCombine ? (BinaryOperator)arguments[2] : null;
			Function keyMapper = (Function)arguments[0];
			Function valueMapper = (Function)arguments[1];
			currentTask.andThen(new KeyValueMappingFunction(keyMapper, valueMapper, aggregator));
			
			// common
			this.taskChain.add(this.createTaskDescriptor(invocation));
			TaskDescriptor curresntTaskDescriptor = this.getCurrentTask(invocation);
			curresntTaskDescriptor.compose(new ValuesReducingFunction((BinaryOperator)arguments[2]));
		}
		else if (operationName.equals("group")) {
			BinaryOperator aggregator = null;
			Function keyMapper = (Function)arguments[0];
			Function valueMapper = arguments.length == 2 ? (Function)arguments[1] : s -> s;
			currentTask.andThen(new KeyValueMappingFunction(keyMapper, valueMapper, aggregator));
			
			// common	
			this.taskChain.add(new TaskDescriptor(this.sequenceIdCounter++, this.invocationChain.getSourceIdentifier(), invocation.getMethod().getName()));
			TaskDescriptor curresntTaskDescriptor = this.getCurrentTask(invocation);
			curresntTaskDescriptor.compose(new ValuesGroupingFunction(Aggregators::aggregateFlatten));
		}
		else if (operationName.equals("join")) {
	
			StreamInvocationChain dependentInvocationChain = (StreamInvocationChain) arguments[0];
			TaskDescriptorChainBuilder dependentBuilder = new TaskDescriptorChainBuilder(this.executionName, dependentInvocationChain, this.executionConfig);
			List<TaskDescriptor> dependentTasks = dependentBuilder.build();

			String shuffleOperationName =  currentTask.getShuffleOperationName();
			
			if (!shuffleOperationName.equals("join")){
				this.taskChain.add(new TaskDescriptor(this.sequenceIdCounter++, this.invocationChain.getSourceIdentifier(), operationName));
			}
			TaskDescriptor joinDescriptor = this.getCurrentTask(invocation);
			joinDescriptor.addDependentTasksChain(Tuple2.tuple2((Predicate)arguments[1], dependentTasks));
		}
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' temporarily is not supported");
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void processIntermediateOperation(MethodInvocation invocation){	
		Function<Stream<?>, Stream<?>> function = invocation.getMethod().getName().equals("compute") 
				? (Function<Stream<?>, Stream<?>>) invocation.getArguments()[0]
						: new DStreamToStreamAdapterFunction(invocation.getMethod().getName(), invocation.getArguments()[0]);

		TaskDescriptor task = this.getCurrentTask(invocation);
		task.andThen(function);
	}
	
	/**
	 * 
	 */
	private boolean isIntermediateOperation(String operationName){
		return operationName.equals("flatMap") || 
			   operationName.equals("map") || 
			   operationName.equals("filter") ||
			   operationName.equals("compute");
	}
	
	/**
	 * 
	 */
	private boolean isShuffleOperation(String operationName){
		return operationName.equals("group") ||
			   operationName.equals("reduceGroups") ||
			   operationName.equals("aggregateGroups") ||
			   operationName.equals("join") ||
			   operationName.equals("partition");
	}
	
	/**
	 * 
	 */
	private TaskDescriptor getCurrentTask(MethodInvocation invocation){
		if (this.taskChain.size() == 0){
			this.taskChain.add(this.createTaskDescriptor(invocation));
		}
		return this.taskChain.get(this.taskChain.size() - 1);
	}
	
	/**
	 * 
	 */
	private TaskDescriptor createTaskDescriptor(MethodInvocation invocation){
		TaskDescriptor taskDescriptor = new TaskDescriptor(this.sequenceIdCounter++, this.invocationChain.getSourceIdentifier(), invocation.getMethod().getName());
		return taskDescriptor;
	}
}

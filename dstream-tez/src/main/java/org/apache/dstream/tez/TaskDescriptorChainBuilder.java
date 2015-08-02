package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.DistributableConstants;
import org.apache.dstream.StreamInvocationChain;
import org.apache.dstream.function.DStreamToStreamAdapterFunction;
import org.apache.dstream.function.HashPartitionerFunction;
import org.apache.dstream.function.KeyValueMappingFunction;
import org.apache.dstream.function.PartitionerFunction;
import org.apache.dstream.function.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.function.StreamJoinerFunction;
import org.apache.dstream.function.ValuesGroupingFunction;
import org.apache.dstream.function.ValuesReducingFunction;
import org.apache.dstream.support.Aggregators;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.ReflectionUtils;

/**
 * 
 */
class TaskDescriptorChainBuilder {
	
	private final List<TaskDescriptor> taskChain;
	
	private final StreamInvocationChain invocationChain;
	
	private final String executionName;
	
	private final Properties executionConfig;
	
	private int sequenceIdCounter;
	
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
		
		if (invocations.size() == 0){
			TaskDescriptor td = this.createTaskDescriptor("map");
			this.decorateTask(td);
			Function<Stream<?>, Stream<?>> function = new DStreamToStreamAdapterFunction("map", (Function<?,?>)s -> s);
			td.andThen(function);
			this.taskChain.add(td);
		}
		else {
			for (MethodInvocation invocation : invocations) {
				String operationName = invocation.getMethod().getName();	
				this.addInitialTaskDescriptorIfNecessary(operationName);
				
				if (this.isTransformation(operationName)){
					this.processIntermediateOperation(invocation);
				}
				else if (this.isShuffle(operationName)){		
					this.processShuffleOperation(invocation);
				}
				else {
					if (operationName.equals("on")){
						Function<?,?> f = this.getCurrentTask().getFunction();
						StreamJoinerFunction joiner = (StreamJoinerFunction) f;
						Predicate<?> p = (Predicate<?>) invocation.getArguments()[0];
						joiner.addTransformationOrPredicate("filter", p);
					}
					else {
						// Should never get here since checks will be performed in core. So, this is to complete IF statement only.
						throw new UnsupportedOperationException(operationName);
					}
				}
			}
		}
		return this.taskChain;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void processIntermediateOperation(MethodInvocation invocation){	
		Function<Stream<?>, Stream<?>> function = invocation.getMethod().getName().equals("compute") 
				? (Function<Stream<?>, Stream<?>>) invocation.getArguments()[0]
						: new DStreamToStreamAdapterFunction(invocation.getMethod().getName(), invocation.getArguments()[0]);
				
		TaskDescriptor currentTask = this.getCurrentTask();
		Function<?,?> f = currentTask.getFunction();
		if (f instanceof StreamJoinerFunction){
			StreamJoinerFunction joiner = (StreamJoinerFunction) f;
			joiner.addTransformationOrPredicate(function);
		}
		else {
			this.getCurrentTask().andThen(function);
		}	
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void processShuffleOperation(MethodInvocation invocation){
		String operationName = invocation.getMethod().getName();
		Object[] arguments = invocation.getArguments();
		
		if (operationName.equals("reduceGroups")) {
			TaskDescriptor currentTask = this.getCurrentTask();
			String propertyName = DistributableConstants.MAP_SIDE_COMBINE + currentTask.getId() + "_" + currentTask.getName();
			boolean mapSideCombine = Boolean.parseBoolean((String)this.executionConfig.getOrDefault(propertyName, "false"));
			BinaryOperator aggregator = mapSideCombine ? (BinaryOperator)arguments[2] : null;
			Function keyMapper = (Function)arguments[0];
			Function valueMapper = (Function)arguments[1];
			currentTask.andThen(new KeyValueMappingFunction(keyMapper, valueMapper, aggregator));
			
			// common
			TaskDescriptor newTaskDescriptor = this.createTaskDescriptor(operationName);
			this.taskChain.add(newTaskDescriptor);
			newTaskDescriptor.compose(new ValuesReducingFunction((BinaryOperator)arguments[2]));
		}
		else if (operationName.equals("aggregateGroups")) {
			TaskDescriptor currentTask = this.getCurrentTask();
			String propertyName = DistributableConstants.MAP_SIDE_COMBINE + currentTask.getId() + "_" + currentTask.getName();
			boolean mapSideCombine = Boolean.parseBoolean((String)this.executionConfig.getOrDefault(propertyName, "false"));
			BinaryOperator aggregator = mapSideCombine ? (BinaryOperator)arguments[2] : null;
			Function keyMapper = (Function)arguments[0];
			Function valueMapper = (Function)arguments[1];
			currentTask.andThen(new KeyValueMappingFunction(keyMapper, valueMapper, aggregator));
			
			// common
			TaskDescriptor newTaskDescriptor = this.createTaskDescriptor(operationName);
			this.taskChain.add(newTaskDescriptor);
			newTaskDescriptor.compose(new ValuesGroupingFunction((BinaryOperator)arguments[2]));	
		}
		else if (operationName.equals("group")) {
			TaskDescriptor currentTask = this.getCurrentTask();
			BinaryOperator aggregator = null;
			Function keyMapper = (Function)arguments[0];
			Function valueMapper = arguments.length == 2 ? (Function)arguments[1] : s -> s;
			currentTask.andThen(new KeyValueMappingFunction(keyMapper, valueMapper, aggregator));
			
			// common	
			TaskDescriptor newTaskDescriptor = this.createTaskDescriptor(operationName);
			this.taskChain.add(newTaskDescriptor);
			newTaskDescriptor.compose(new ValuesGroupingFunction(Aggregators::aggregateFlatten));
		}
		else if (operationName.equals("join")) {
			StreamInvocationChain dependentInvocationChain = (StreamInvocationChain) arguments[0];
			TaskDescriptorChainBuilder dependentBuilder = new TaskDescriptorChainBuilder(this.executionName, dependentInvocationChain, this.executionConfig);
			List<TaskDescriptor> dependentTasks = dependentBuilder.build();

			TaskDescriptor currentTask = this.getCurrentTask();
			
			if (currentTask.getId() == 0){
				// create pass through mapper (Tez limitation)
				currentTask.andThen(s -> s);
			}
			if (this.isTransformation(currentTask.getOperationName())){
				TaskDescriptor td = this.createTaskDescriptor(operationName);
				this.taskChain.add(td);
				Function function = new TezJoiner();
				td.andThen(function);
				currentTask = this.getCurrentTask();
			}
			
			currentTask.addDependentTasksChain(dependentTasks);
			
			Function function = currentTask.getFunction();
			StreamJoinerFunction joiner = (StreamJoinerFunction) function;
			int joiningStreamsSize = dependentInvocationChain.getStreamType().getTypeParameters().length;
			joiner.addCheckPoint(joiningStreamsSize);
		}
		else if (operationName.equals("partition")) {
			TaskDescriptor newTaskDescriptor = this.createTaskDescriptor(operationName);
			this.taskChain.add(newTaskDescriptor);
		}
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' temporarily is not supported");
		}
		
		this.setTaskParallelism(this.getCurrentTask());
	}
	
	private void addInitialTaskDescriptorIfNecessary(String operationName) {
		if (this.taskChain.size() == 0){
			if (this.isShuffle(operationName)){
				operationName = "map";
			}
			TaskDescriptor td = this.createTaskDescriptor(operationName);
			this.taskChain.add(td);
			this.decorateTask(td);
		}
	}
	
	/**
	 * 
	 * @param td
	 */
	private void decorateTask(TaskDescriptor td){
		if (td.getName() == null){
			td.setName(this.invocationChain.getSourceIdentifier());
		}
		if (td.getId() == 0 && td.getSourceSupplier() == null){
			String sourceProperty = executionConfig.getProperty(DistributableConstants.SOURCE + this.invocationChain.getSourceIdentifier());
			Assert.notEmpty(sourceProperty, DistributableConstants.SOURCE + this.invocationChain.getSourceIdentifier() +  "' property can not be found in " + 
					this.executionName + ".cfg configuration file.");
			SourceSupplier<?> sourceSupplier = SourceSupplier.create(sourceProperty, null);
			td.setSourceSupplier(sourceSupplier);
			td.setSourceElementType(this.invocationChain.getSourceElementType());
		}
	}
	
	/**
	 * 
	 * @param taskDescriptor
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void setTaskParallelism(TaskDescriptor taskDescriptor){
		String parallelizmProp = this.executionConfig.getProperty(DistributableConstants.PARALLELISM + taskDescriptor.getId() + "_" + taskDescriptor.getName());
		if (parallelizmProp != null){
			String[] pDirective = parallelizmProp.split(",");
			PartitionerFunction partitioner = pDirective.length == 1 ? new HashPartitionerFunction<>(Integer.parseInt(pDirective[0])) 
					: ReflectionUtils.newInstance(pDirective[1], new Class[]{int.class}, new Object[]{Integer.parseInt(pDirective[0])});
			taskDescriptor.setPartitioner(partitioner);
		}
	}
	
	/**
	 * 
	 */
	private boolean isTransformation(String operationName){
		return operationName.equals("flatMap") || 
			   operationName.equals("map") || 
			   operationName.equals("filter") ||
			   operationName.equals("compute");
	}
	
	/**
	 * 
	 */
	private boolean isShuffle(String operationName){
		return operationName.equals("group") ||
			   operationName.equals("reduceGroups") ||
			   operationName.equals("aggregateGroups") ||
			   operationName.equals("join") ||
			   operationName.equals("partition");
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
		TaskDescriptor taskDescriptor = new TaskDescriptor(this.sequenceIdCounter++, this.invocationChain.getSourceIdentifier(), operationName);
		this.decorateTask(taskDescriptor);
		return taskDescriptor;
	}
}

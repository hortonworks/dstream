/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.dstream.DStreamExecutionGraph;
import io.dstream.DStreamOperation;
import io.dstream.support.SourceSupplier;

/**
 *
 */
class TaskDescriptorChainBuilder {

	private final List<TaskDescriptor> taskChain;

	private final DStreamExecutionGraph executionGraph;

	private final String executionName;

	private final Properties executionConfig;

	private int sequenceIdCounter;

	/**
	 *
	 * @param executionName
	 * @param invocationChain
	 * @param executionConfig
	 */
	public TaskDescriptorChainBuilder(String executionName, DStreamExecutionGraph executionGraph, Properties executionConfig){
		this.taskChain = new ArrayList<>();
		this.executionGraph = executionGraph;
		this.executionName = executionName;
		this.executionConfig = executionConfig;
	}

	/**
	 *
	 * @return
	 */
	public List<TaskDescriptor> build(){
		List<DStreamOperation> streamOperations = this.executionGraph.getOperations();

		for (DStreamOperation streamOperation : streamOperations) {
			TaskDescriptor taskDescriptor;
			if (streamOperation.getLastOperationName().equals("join") || streamOperation.getLastOperationName().startsWith("union")){
				String name = this.taskChain.get(this.taskChain.size() - 1).getOperationName();
				if (!name.contains("classify")){
					throw new IllegalStateException("Unclassified stream combines (join/union) are not supported at the moment by Tez.");
				}
			}
			if (!streamOperation.getCombinableExecutionGraphs().isEmpty()){
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
		for (DStreamExecutionGraph dependentOps : streamOperation.getCombinableExecutionGraphs()) {
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
			//			String sourceProperty = this.executionConfig.getProperty(DStreamConstants.SOURCE + this.executionGraph.getName());

			//			Assert.notEmpty(sourceProperty, DStreamConstants.SOURCE + this.executionGraph.getName() +  "' property can not be found in " +
			//					this.executionName + ".cfg configuration file.");
			SourceSupplier<?> sourceSupplier = SourceSupplier.create(this.executionConfig, td.getName(), null);
			td.setSourceSupplier(sourceSupplier);
			td.setSourceElementType(this.executionGraph.getSourceElementType());
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
				this.executionGraph.getName(), operationName, this.executionConfig, this.getCurrentTask());
		this.initializeTaskInputsIfNecessary(taskDescriptor);
		return taskDescriptor;
	}
}
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
import java.util.stream.Stream;

import org.apache.tez.dag.api.Vertex;

import io.dstream.DStreamConstants;
import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.SerializableStreamAssets.SerSupplier;
import io.dstream.support.Classifier;
import io.dstream.support.HashClassifier;
import io.dstream.utils.ReflectionUtils;

/**
 *
 *
 */
public class TaskDescriptor {

	private final String name;

	private final int id;

	private final TaskDescriptor previousTaskDescriptor;

	private final String operationName;


	private SerFunction<Stream<?>, Stream<?>> function;

	private Classifier classifier;

	private int parallelism = 1;

	private Class<?> sourceElementType;

	private SerSupplier<?> sourceSupplier;

	private List<List<TaskDescriptor>> dependentTasksChains;

	private Class<?> inputFormatClass;

	/**
	 * Will create description of a {@link Task} from which Tez {@link Vertex} is created.
	 * Parallelism and {@link Classifier} of the task (Vertex) is determined
	 * from {@link DStreamConstants#PARALLELISM} configuration
	 * which allows to configure both parallelism and {@link Classifier}. However, due to Tez way of
	 * doing things, the actual function itself should be applied to the previous task (Vertex)
	 * where the actual partitioning logic is invoked, while integer value representing parallelism should *also*
	 * be set on the current Vertex.
	 * To accommodate that the {@link TaskDescriptor} is created with reference to the previous
	 * {@link TaskDescriptor}. Upon determining partitioner configuration and parallelism for the current task,
	 * the actual {@link Classifier} is created and set on the previous {@link TaskDescriptor} while
	 * it's parallelism is set on this task.
	 *
	 * @param id
	 * @param name
	 * @param operationName
	 * @param executionConfig
	 * @param previousTaskDescriptor
	 */
	public TaskDescriptor(int id, String name, String operationName, Properties executionConfig, TaskDescriptor previousTaskDescriptor){
		this.name = name;
		this.id = id;
		this.operationName = operationName;
		this.previousTaskDescriptor = previousTaskDescriptor;
		String parallelizmProp = executionConfig.getProperty(DStreamConstants.PARALLELISM);
		String grouperProp = executionConfig.getProperty(DStreamConstants.CLASSIFIER);

		if (parallelizmProp != null){
			this.parallelism = Integer.parseInt(parallelizmProp);
		}
		Classifier classifier = grouperProp != null
				? ReflectionUtils.newInstance(grouperProp, new Class[]{int.class}, new Object[]{this.parallelism})
						: new HashClassifier(this.parallelism);
				this.setClassifier(classifier);
	}

	/**
	 *
	 * @return
	 */
	public TaskDescriptor getPreviousTaskDescriptor() {
		return previousTaskDescriptor;
	}

	/**
	 *
	 * @return
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 *
	 * @return
	 */
	public List<List<TaskDescriptor>> getDependentTasksChains() {
		return this.dependentTasksChains;
	}

	/**
	 *
	 * @return
	 */
	public String getOperationName() {
		return operationName;
	}

	/**
	 *
	 * @param dependentTasksChain
	 */
	public void addDependentTasksChain(List<TaskDescriptor> dependentTasksChain) {
		if (this.dependentTasksChains == null){
			this.dependentTasksChains = new ArrayList<>();
		}
		this.dependentTasksChains.add(dependentTasksChain);
	}

	/**
	 *
	 * @return
	 */
	public Class<?> getInputFormatClass() {
		return inputFormatClass;
	}

	/**
	 *
	 * @param inputFormatClass
	 */
	public void setInputFormatClass(Class<?> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}

	/**
	 *
	 * @return
	 */
	public SerSupplier<?> getSourceSupplier() {
		return this.sourceSupplier;
	}

	/**
	 *
	 * @return
	 */
	public int getId() {
		return this.id;
	}

	/**
	 *
	 * @return
	 */
	public Classifier getClassifier() {
		return this.classifier;
	}

	/**
	 *
	 * @return
	 */
	public SerFunction<Stream<?>, Stream<?>> getFunction() {
		return this.function;
	}

	/**
	 *
	 * @param cFunction
	 */
	public void compose(SerFunction<Stream<?>, Stream<?>> cFunction) {
		if (this.function != null){
			this.function = this.function.compose(cFunction);
		}
		else {
			this.function = cFunction;
		}
	}

	/**
	 *
	 * @param aFunction
	 */
	public void andThen(SerFunction<Stream<?>, Stream<?>> aFunction) {
		if (this.function != null){
			this.function = aFunction.compose(this.function);
		}
		else {
			this.function = aFunction;
		}
	}

	/**
	 *
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 *
	 * @return
	 */
	public Class<?> getSourceElementType() {
		return sourceElementType;
	}

	/**
	 *
	 */
	void setSourceElementType(Class<?> sourceElementType) {
		this.sourceElementType = sourceElementType;
	}

	/**
	 *
	 */
	void setClassifier(Classifier classifier) {
		this.classifier = classifier;
	}

	/**
	 *
	 */
	void setSourceSupplier(SerSupplier<?> sourceSupplier) {
		this.sourceSupplier = sourceSupplier;
	}
}

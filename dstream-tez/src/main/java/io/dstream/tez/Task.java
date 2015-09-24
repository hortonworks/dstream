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

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.support.AbstractPartitionedStreamProducingSourceSupplier;
import io.dstream.support.Classifier;

/**
 *
 */
final class Task implements Serializable {
	private static final long serialVersionUID = -1800812882885490376L;

	private final SerFunction<Stream<?>, Stream<?>> function;

	private final Classifier classifier;

	private final String name;

	private final int id;

	private AbstractPartitionedStreamProducingSourceSupplier<?> streamProducingSourceSupplier;

	/**
	 *
	 * @param id
	 * @param name
	 * @param partitioner
	 * @param function
	 */
	private Task(int id, String name, Classifier classifier, SerFunction<Stream<?>, Stream<?>> function){
		this.id = id;
		this.name = name;
		this.classifier = classifier;
		this.function = function;
	}

	/**
	 *
	 * @param taskDescriptor
	 * @return
	 */
	static Task build(TaskDescriptor taskDescriptor) {
		SerFunction<Stream<?>, Stream<?>> taskFunction = adjustTaskFunction(taskDescriptor);
		Task task = new Task(taskDescriptor.getId(), taskDescriptor.getName(), taskDescriptor.getClassifier(), taskFunction);
		if (taskDescriptor.getSourceSupplier() instanceof AbstractPartitionedStreamProducingSourceSupplier){
			task.setStreamProducingSourceSupplier((AbstractPartitionedStreamProducingSourceSupplier<?>) taskDescriptor.getSourceSupplier());
		}
		return task;
	}

	/**
	 *
	 * @return
	 */
	public SerFunction<Stream<?>, Stream<?>> getFunction() {
		return function;
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
	public String getName() {
		return name;
	}

	/**
	 *
	 * @return
	 */
	public int getId() {
		return id;
	}

	/**
	 *
	 * @return
	 */
	public AbstractPartitionedStreamProducingSourceSupplier<?> getStreamProducingSourceSupplier() {
		return streamProducingSourceSupplier;
	}

	/**
	 *
	 * @param streamProducingSourceSupplier
	 */
	void setStreamProducingSourceSupplier(AbstractPartitionedStreamProducingSourceSupplier<?> streamProducingSourceSupplier) {
		this.streamProducingSourceSupplier = streamProducingSourceSupplier;
	}

	/**
	 * This will adjust task function to ensure that it is compatible with Hadoop KV readers and types expected by user.
	 * For example, reading Text file Tez will produce KV pairs (offset, line), while user is only expected the value.
	 */
	@SuppressWarnings("rawtypes")
	private static SerFunction<Stream<?>, Stream<?>> adjustTaskFunction(TaskDescriptor taskDescriptor){
		SerFunction<Stream<?>, Stream<?>> modifiedFunction = taskDescriptor.getFunction();
		if (taskDescriptor.getId() == 0 && !Entry.class.isAssignableFrom(taskDescriptor.getSourceElementType())){
			if (Writable.class.isAssignableFrom(taskDescriptor.getSourceElementType())){
				modifiedFunction = modifiedFunction.compose(stream -> stream.map(s -> ((Entry)s).getValue()));
			}
			else {
				if (taskDescriptor.getInputFormatClass() != null){// only URI based sources will have Input Format
					ParameterizedType parameterizedType = (ParameterizedType) taskDescriptor.getInputFormatClass().getGenericSuperclass();
					Type type = parameterizedType.getActualTypeArguments()[1];

					if (Text.class.getName().equals(type.getTypeName())){
						if (modifiedFunction == null) {
							modifiedFunction = stream -> stream.map(s -> ((Entry) s).getValue().toString());
						} else {
							modifiedFunction = modifiedFunction.compose(stream -> stream.map(s -> ((Entry) s)
									.getValue().toString()));
						}
					}
					else {
						//TODO need to design some type of extensible converter to support multiple types of Writable
						throw new IllegalStateException("Can't determine modified function");
					}
				}
			}
		}
		return modifiedFunction;
	}
}

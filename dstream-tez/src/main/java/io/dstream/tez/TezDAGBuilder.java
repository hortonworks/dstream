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

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

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

import io.dstream.SerializableStreamAssets.SerSupplier;
import io.dstream.support.SourceSupplier;
import io.dstream.support.UriSourceSupplier;
import io.dstream.tez.io.KeyWritable;
import io.dstream.tez.io.TezDelegatingPartitioner;
import io.dstream.tez.io.ValueWritable;
import io.dstream.tez.utils.HdfsSerializerUtils;

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
	 * @param executionName
	 * @param tezClient
	 * @param executionConfig
	 */
	public TezDAGBuilder(String executionName, ExecutionContextAwareTezClient tezClient, Properties executionConfig) {
		this.dag = DAG.create(executionName + "_" + System.currentTimeMillis());
		this.tezClient = tezClient;

		// TODO need to figure out when and why would the Edge be different and
		// how to configure it
		this.edgeConf = OrderedPartitionedKVEdgeConfig.newBuilder("io.dstream.tez.io.KeyWritable",
				"io.dstream.tez.io.ValueWritable", TezDelegatingPartitioner.class.getName(), null).build();
		this.dagExecutor = new TezDagExecutor(this.tezClient, this.dag);
	}

	/**
	 *
	 * @param taskDescriptor
	 */
	public void addTask(TaskDescriptor taskDescriptor) {
		if (taskDescriptor.getId() == 0) {
			this.determineInputFormatClass(taskDescriptor);
		}
		UserPayload payload = this.createPayloadFromTaskSerPath(Task.build(taskDescriptor), this.dag.getName());
		ProcessorDescriptor pd = ProcessorDescriptor.create(TezTaskProcessor.class.getName()).setUserPayload(payload);
		SerSupplier<?> sourceSupplier = taskDescriptor.getSourceSupplier();

		Vertex vertex = this.createVertex(taskDescriptor, pd);

		this.dag.addVertex(vertex);

		if (taskDescriptor.getId() == 0) {
			if (sourceSupplier instanceof UriSourceSupplier) {
				UriSourceSupplier uriSourceSupplier = (UriSourceSupplier) sourceSupplier;
				Stream<URI> uris = uriSourceSupplier.get();
				DataSourceDescriptor dataSource = this.buildDataSourceDescriptorFromUris(taskDescriptor.getInputFormatClass(), uris);
				vertex.addDataSource(this.inputOrderCounter++ + ":" + vertex.getName() + "_INPUT_" + Arrays.asList(uris), dataSource);
			}
		}
		else {
			this.addEdge(vertex);
		}

		if (taskDescriptor.getDependentTasksChains() != null) {
			List<List<TaskDescriptor>> dependentTasksChains = taskDescriptor.getDependentTasksChains();
			dependentTasksChains.forEach(dependentTasks -> {
				dependentTasks.forEach(this::addTask);
				this.addEdge(vertex);
			});
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Created Vertex: " + vertex);
		}
		this.lastVertex = vertex;
	}

	/**
	 *
	 */
	private Vertex createVertex(TaskDescriptor taskDescriptor, ProcessorDescriptor pd) {
		String vertexName = taskDescriptor.getName() + "_" + taskDescriptor.getOperationName();
		Vertex vertex = (taskDescriptor.getId() == 0 && taskDescriptor.getSourceSupplier() instanceof UriSourceSupplier)
				? Vertex.create(this.inputOrderCounter++ + ":" + vertexName, pd)
						: Vertex.create(this.inputOrderCounter++ + ":" + vertexName, pd, taskDescriptor.getParallelism());
				vertex.addTaskLocalFiles(this.tezClient.getLocalResources());
				return vertex;
	}

	/**
	 *
	 * @param vertex
	 */
	private void addEdge(Vertex vertex) {
		Edge edge = Edge.create(this.lastVertex, vertex, this.edgeConf.createDefaultEdgeProperty());
		this.dag.addEdge(edge);
	}

	/**
	 *
	 */
	public void addDataSink(String outputPath) {
		this.createDataSink(this.lastVertex, this.tezClient.getClientName() + "_OUTPUT", KeyWritable.class,
				ValueWritable.class, SequenceFileOutputFormat.class, outputPath);

		this.lastVertex = null;
	}

	/**
	 *
	 * @return
	 */
	public Runnable build() {
		return this.dagExecutor;
	}

	/**
	 *
	 */
	private DataSourceDescriptor buildDataSourceDescriptorFromUris(Class<?> inputFormatClass, Stream<URI> sources) {
		String inputPath = sources.map(uri -> uri.getPath()).reduce((a,b) -> a + "," + b).get();
		return MRInput.createConfigBuilder(this.tezClient.getTezConfiguration(), inputFormatClass, inputPath)
				.groupSplits(false).build();
	}

	/**
	 *
	 */
	private UserPayload createPayloadFromTaskSerPath(Task task, String dagName) {
		org.apache.hadoop.fs.Path mapTaskPath = HdfsSerializerUtils.serialize(task, this.tezClient.getFileSystem(),
				new org.apache.hadoop.fs.Path(dagName + "/tasks/" + task.getId() + "_" + task.getName() + ".ser"));
		return UserPayload.create(ByteBuffer.wrap(mapTaskPath.toString().getBytes()));
	}

	/**
	 *
	 */
	private void createDataSink(Vertex vertex, String name, Class<? extends Writable> keyClass,
			Class<? extends Writable> valueClass, Class<?> outputFormatClass, String outputPath) {
		JobConf dsConfig = this.buildJobConf(keyClass, valueClass);
		DataSinkDescriptor dataSink = MROutput.createConfigBuilder(dsConfig, outputFormatClass, outputPath).build();
		vertex.addDataSink(name, dataSink);
	}

	/**
	 *
	 */
	private JobConf buildJobConf(Class<? extends Writable> keyClass, Class<? extends Writable> valueClass) {
		JobConf jobConf = new JobConf(this.tezClient.getTezConfiguration());
		jobConf.setOutputKeyClass(keyClass);
		jobConf.setOutputValueClass(valueClass);
		return jobConf;
	}

	/**
	 *
	 */
	private void determineInputFormatClass(TaskDescriptor firstTask) {
		SourceSupplier<?> sourceSupplier = (SourceSupplier<?>) firstTask.getSourceSupplier();
		Class<?> sourceElementType = firstTask.getSourceElementType();
		if (sourceSupplier instanceof UriSourceSupplier) {
			if (sourceElementType.isAssignableFrom(String.class)) {
				firstTask.setInputFormatClass(TextInputFormat.class);
			}
			else {
				// TODO design a configurable component to handle other standard
				// and custom input types
				throw new IllegalArgumentException("Failed to determine Input Format class for source item type " + sourceElementType);
			}
		}
		//		else {
		//			throw new IllegalArgumentException("Non URI sources are not supported yet");
		//		}
	}
}

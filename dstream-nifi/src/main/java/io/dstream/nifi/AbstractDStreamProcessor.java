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
package io.dstream.nifi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import io.dstream.DStream;
import io.dstream.DStreamConstants;
import io.dstream.utils.Assert;

/**
 * Base implementation of the {@link Processor} to support {@link DStream}
 * applications.
 *
 * It handles most common functionality required by the {@link DStream} and
 * sub-classes are only required to implement {@link #getDStream(String)} method,
 * which returns an instance of an executable {@link DStream}.<br>
 * <p>
 * This {@link Processor} is an <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/EventDrivenConsumer.html">Event Driven Consumer</a>
 * and it's triggered by an arrival of the execution configuration file (e.g., WordCount.cfg).
 * Once configuration file is arrived it's added to the current class-path and an attempt is made to get an
 * instance of a {@link DStream} (see {@link #getDStream(String)}) for a specific execution
 * name - determined based on the name of the configuration file minus extension
 * (e.g., WordCount.cfg -&gt; 'WordCount'). <br>
 * <p>
 * <i>Basically, the {@link #getDStream(String)} method allows you to host multiple {@link DStream}
 * implementations within a single NAR bundle essentially grouping them based on some criteria.</i>
 * <p>
 * Once the executable {@link DStream} is determined, it's executed and its output path
 * is written as an attribute to the downstream {@link FlowFile} to ensure downstream components
 * will have access to the results of the execution. For additional convenience there is also
 * a {@link #postProcessResults(Stream)} method that could be implemented by a sub-class
 * if there is a need to gain access to the results before they are sent downstream (e.g., testing)<br>
 * <p>
 * This {@link Processor} defines a single configuration property - \"Execution completion timeout (milliseconds)\"
 * with default value of 0. This property indicates how long to wait for completion of {@link DStream} execution.
 * While it has a default value, it is <b>highly recommended</b> to set it to a more realistic value indicating
 * how long are you willing to wait for the result completion. With default value it will wait indefinitely.
 */
@EventDriven
public abstract class AbstractDStreamProcessor extends AbstractProcessor {

	public static final Relationship OUTPUT = new Relationship.Builder().name("success")
			.description("Upon successfull completion of DStream execution, its output path is forwarded to success").build();

	public static final PropertyDescriptor EXECUTION_COMPLETION_TIMEOUT = new PropertyDescriptor.Builder()
			.name("Execution completion timeout (milliseconds)")
			.description("Indicates how long to wait for completion of DStream execution. Defaults to 0 (wait indefinitely).")
			.addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
			.defaultValue("0")
			.required(true)
			.build();

	private volatile Set<Relationship> relationships;

	private volatile List<PropertyDescriptor> properties;

	/**
	 *
	 */
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final ProcessorLog log = this.getLogger();
		long executionCompletionTimeout = Long.parseLong(context.getProperty(EXECUTION_COMPLETION_TIMEOUT).getValue());

		FlowFile flowFile = session.get();
		if (flowFile != null){
			try {
				String configurationName = flowFile.getAttribute("filename");
				Assert.isTrue(configurationName.endsWith(".cfg"), "Received invalid configuration file '" +
						configurationName + "'. DStream configuration file must end with '.cfg'.");

				log.info("Recieved configuration '" + configurationName + "'");
				AtomicReference<String> outputPathRef = new AtomicReference<String>();
				session.read(flowFile, new InputStreamCallback() {
					@Override
					public void process(InputStream confFileInputStream) throws IOException {
						outputPathRef.set(installConfiguration(configurationName, confFileInputStream));
					}
				});
				String executionName = configurationName.split("\\.")[0];

				DStream<?> dstream = this.getDStream(executionName);
				if (dstream != null){
					log.info("Executing DStream for '" + executionName + "'");
					this.postProcessResults(this.executeDStream(dstream, executionName, executionCompletionTimeout));
					FlowFile resultFlowFile = session.create();
					resultFlowFile = session.putAttribute(resultFlowFile, CoreAttributes.FILENAME.key(), outputPathRef.get());
					session.getProvenanceReporter().receive(resultFlowFile, outputPathRef.get());
					session.transfer(resultFlowFile, OUTPUT);
				} else {
					log.warn("Failed to locate DStream for execution '" + executionName + "'"
							+ ". Nothing was executed. Possible reasons: " + this.getClass().getSimpleName()
							+ " may not have provided a DStream for '" + executionName + "'");
				}
			}
			catch (Exception e) {
				throw new IllegalStateException("Failed DStream execution with unexpected exception ", e);
			}
			finally {
				session.remove(flowFile);
				session.commit();
			}
		}
	}

	/**
	 *
	 */
	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	/**
	 *
	 */
	@Override
	protected void init(final ProcessorInitializationContext context) {
		Set<Relationship> relationships = new HashSet<>();
		relationships.add(OUTPUT);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(EXECUTION_COMPLETION_TIMEOUT);
		this.properties = Collections.unmodifiableList(properties);
	}

	/**
	 *
	 */
	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	/**
	 * Returns an instance of the {@link DStream} for a given execution name.
	 * May return 'null'.
	 * @param executionName the execution name used when invoking {@link DStream#executeAs(String)}
	 *                      operation.
	 */
	protected abstract <T> DStream<T> getDStream(String executionName);

	/**
	 * Gives you a handle to execution result partitions (see {@link DStream#executeAs(String)} )<br>
	 * Typically used for testing/debugging
	 * @param resultPartitions {@link Stream} of {@link Stream}s where each represents an individual partition.
	 */
	protected <T> void postProcessResults(Stream<Stream<T>> resultPartitions) {
		// default NOOP
	}

	/**
	 *
	 * @param dstream
	 */
	@SuppressWarnings("unchecked")
	private <T> Stream<Stream<T>> executeDStream(DStream<?> dstream, String executionName, long executionCompletionTimeout) {
		ProcessorLog log = this.getLogger();
		Future<?> resultFuture = dstream.executeAs(executionName);
		try {
			if (executionCompletionTimeout > 0){
				return (Stream<Stream<T>>) resultFuture.get(executionCompletionTimeout, TimeUnit.MILLISECONDS);
			}
			else {
				log.warn("Waiting for completion of '" + executionName + "' indefinitely. "
						+ "Consider setting 'Execution completion timeout' property of your processor"
						+ "when configured via UI." );
				return (Stream<Stream<T>>) resultFuture.get();
			}
		}
		catch (InterruptedException | ExecutionException | TimeoutException e) {
			if (e instanceof InterruptedException){
				Thread.currentThread().interrupt();
			}
			throw new IllegalStateException("Failed while waiting for execution to complete", e);
		}
	}

	/**
	 * Will generate execution configuration and add it to the classpath
	 * @param context
	 */
	private String installConfiguration(String configurationName, InputStream confFileInputStream){
		String outputPath;
		try {
			File confDir = new File(System.getProperty("java.io.tmpdir") + "/dstream_" + UUID.randomUUID());
			confDir.mkdirs();
			File executionConfig = new File(confDir, configurationName);
			executionConfig.deleteOnExit();
			FileOutputStream confFileOs = new FileOutputStream(executionConfig);

			Properties configurationProperties = new Properties();
			configurationProperties.load(confFileInputStream);
			configurationProperties.store(confFileOs, configurationName + " configuration");

			this.addToClassPath(confDir);

			outputPath = configurationProperties.containsKey(DStreamConstants.OUTPUT)
					? configurationProperties.getProperty(DStreamConstants.OUTPUT)
							: configurationName.split("\\.")[0] + "/out";
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to generate execution config", e);
		}
		return outputPath;
	}

	/**
	 *
	 */
	private void addToClassPath(File configurationDir){
		try {
			URLClassLoader dstreamCl = URLClassLoader
					.newInstance(new URL[]{configurationDir.toURI().toURL()}, Thread.currentThread().getContextClassLoader());
			Thread.currentThread().setContextClassLoader(dstreamCl);
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to update classpath with path '" + configurationDir + "'.", e);
		}
	}
}

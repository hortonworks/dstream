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
package org.apache.nifi.dstream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import dstream.DStream;
import dstream.DStreamConstants;
import dstream.utils.Assert;
import dstream.utils.PropertiesHelper;

/**
 * 
 */
@Tags({ "dstream", "integration" })
@CapabilityDescription("This processor loads and executes DStream processes")
public abstract class AbstractDStreamProcessor extends AbstractProcessor {

	public static final Relationship OUTPUT = new Relationship.Builder().name("success")
			.description("Upon successfull completion of DStream execution, its output path is forwarded to success").build();
	
	public static final PropertyDescriptor EXECUTION_COMPLETION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Execution completion timeout (milliseconds)")
            .description("Indicates how long to wait for completion of DStream execution. Defaults to 0 (wait indefinitely).")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
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
						configurationName + "'. DStream configuration file must end with cfg");
				
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
					this.executeDStream(dstream, executionName, executionCompletionTimeout);
					FlowFile resultFlowFile = session.create();
					resultFlowFile = session.putAttribute(resultFlowFile, CoreAttributes.FILENAME.key(), outputPathRef.get());
					session.getProvenanceReporter().receive(resultFlowFile, outputPathRef.get());
			        session.transfer(resultFlowFile, OUTPUT);
				}
				else {
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
	 * 
	 * @param executionName
	 * @return
	 */
	protected abstract <T> DStream<T> getDStream(String executionName);
	
	/**
	 * 
	 * @param dstream
	 */
	private <T> void executeDStream(DStream<T> dstream, String executionName, long executionCompletionTimeout) {
		ProcessorLog log = this.getLogger();
		Future<?> resultFuture = dstream.executeAs(executionName); 
		try {
			resultFuture.get(executionCompletionTimeout, TimeUnit.MILLISECONDS);
		} 
		catch (InterruptedException | ExecutionException | TimeoutException e) {
			if (e instanceof InterruptedException){
				Thread.currentThread().interrupt();
			}
			log.error("Faild while waiting for execution to complete", e);
		}
	}
	
	/**
	 * Will generate execution configuration and add it to the classpath
	 * @param context
	 */
	private String installConfiguration(String configurationName, InputStream confFileInputStream){
		String outputPath;
		try {
			File confDir = new File(System.getProperty("java.io.tmpdir"));
			File executionConfig = new File(confDir, configurationName);
			executionConfig.deleteOnExit();
			FileOutputStream confFileOs = new FileOutputStream(executionConfig);
			IOUtils.copy(confFileInputStream, confFileOs);
			this.updateClassPath(confDir);
			Properties configurationProperties = PropertiesHelper.loadProperties(executionConfig.getName());
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
	 * @param files
	 */
	private void updateClassPath(File... files){
		try {
			URL[] urls = Stream.of(files).map(file -> {
				try {
					return file.toURI().toURL();
				} 
				catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}).collect(Collectors.toList()).toArray(new URL[]{});
			URLClassLoader dstreamCl = URLClassLoader.newInstance(urls, Thread.currentThread().getContextClassLoader());
			Thread.currentThread().setContextClassLoader(dstreamCl);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to update classpath with configuration " + Arrays.asList(files), e);
		}
	}
}

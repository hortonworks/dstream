package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.tez.utils.HadoopUtils;
import org.apache.dstream.tez.utils.SequenceFileOutputStreamsBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dstream.AbstractDStreamExecutionDelegate;
import dstream.DStreamConstants;
import dstream.DStreamInvocationPipeline;

/**
 * Implementation of {@link StreamExecutionDelegate} for Apache Tez.
 *
 */
public class TezExecutionDelegate extends AbstractDStreamExecutionDelegate {
	
	private final Logger logger = LoggerFactory.getLogger(TezExecutionDelegate.class);
	
	private final List<List<TaskDescriptor>> taskChains;
	
	private ExecutionContextAwareTezClient tezClient;
	
	/**
	 * 
	 */
	public TezExecutionDelegate(){
		this.taskChains = new ArrayList<>();
	}

	/**
	 * 
	 */
	@Override
	public Runnable getCloseHandler() {
		return new Runnable() {
			@Override
			public void run() {
				try {
					logger.info("Stopping TezClient");
					tezClient.clearAppMasterLocalFiles();
					tezClient.stop();
				} 
				catch (Exception e) {
					logger.warn("Failed to stop TezClient", e);
				}
			}
		};
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Stream<Stream<?>> doExecute(String executionName, Properties executionConfig, DStreamInvocationPipeline... invocationPipelines) {		
		for (DStreamInvocationPipeline invocationPipeline : invocationPipelines) {
			TaskDescriptorChainBuilder builder = new TaskDescriptorChainBuilder(executionName, invocationPipeline, executionConfig);
			List<TaskDescriptor> taskDescriptors = builder.build();
			this.taskChains.add(taskDescriptors);
		}
		
		TezConfiguration tezConfiguration = new TezConfiguration(new Configuration());
		FileSystem fs = HadoopUtils.getFileSystem(tezConfiguration);
		
		if (this.tezClient == null){
			this.createAndTezClient(executionName, fs, tezConfiguration);
		}
		
		TezDAGBuilder dagBuilder = new TezDAGBuilder(executionName, this.tezClient, executionConfig);
		List<String> outputURIs  = new ArrayList<String>();
		
		String output = (String) executionConfig.getOrDefault(DStreamConstants.OUTPUT, this.tezClient.getClientName() + "/out/");
		for (int i = 0; i < this.taskChains.size(); i++) {
			List<TaskDescriptor> taskChain = this.taskChains.get(i);
			taskChain.forEach(task -> dagBuilder.addTask(task));
			output += (this.taskChains.size() > 1 ? "/" + i : "");
			dagBuilder.addDataSink(output);
			outputURIs.add(output);
		}
		
		Runnable executable = dagBuilder.build();
		
		try {
			executable.run();
			Stream<Stream<?>>[] resultStreams = outputURIs.stream().map(uri -> {
				SequenceFileOutputStreamsBuilder<?> ob = new SequenceFileOutputStreamsBuilder<>(this.tezClient.getFileSystem(), uri, this.tezClient.getTezConfiguration());
				return Stream.of(ob.build());
			}).collect(Collectors.toList()).toArray(new Stream[]{});
			
			if (resultStreams.length == 1){
				return resultStreams[0];
			}
			else {
				return Stream.of(resultStreams);
			}
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to execute DAG for " + executionName, e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	//TODO needs some integration with extarnal conf to get credentials
	protected Credentials getCredentials(){
		return null;
	}

	/**
	 * 
	 * @param pipelineSpecification
	 */
	private void createAndTezClient(String executionName, FileSystem fs, TezConfiguration tezConfiguration){	
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, executionName + 
												"/" + TezConstants.CLASSPATH_PATH);
		this.tezClient = new ExecutionContextAwareTezClient(executionName, 
											   tezConfiguration, 
											   localResources, 
											   this.getCredentials(),
											   fs);
		try {
			this.tezClient.start();
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to start TezClient", e);
		}
	}
}

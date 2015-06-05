package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DistributableConstants;
import org.apache.dstream.ExecutionDelegate;
import org.apache.dstream.ExecutionSpec;
import org.apache.dstream.ExecutionSpec.Stage;
import org.apache.dstream.support.PipelineConfigurationHelper;
import org.apache.dstream.tez.utils.HadoopUtils;
import org.apache.dstream.tez.utils.SequenceFileOutputStreamsBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *
 */
public class TezPipelineExecutionDelegate implements ExecutionDelegate {

	private final Logger logger = LoggerFactory.getLogger(TezPipelineExecutionDelegate.class);
	
	private ExecutionContextAwareTezClient tezClient;
	
	private Properties pipelineConfig;
	
	/**
	 * 
	 */
	@Override
	public Stream<Stream<?>>[] execute(String executionName, ExecutionSpec... pipelineSpecification) {
		try {
			return this.doExecute(executionName, pipelineSpecification);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to execute pipeline: " + pipelineSpecification, e);
		}
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
	 * @return
	 */
	//TODO needs some integration with extarnal conf to get credentials
	protected Credentials getCredentials(){
		return null;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Stream<Stream<?>>[] doExecute(String executionName, ExecutionSpec... pipelineExecutionChains) throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing: " + pipelineExecutionChains);
		}
		
		TezConfiguration tezConfiguration = new TezConfiguration(new Configuration());
		FileSystem fs = HadoopUtils.getFileSystem(tezConfiguration);
		
		this.pipelineConfig = PipelineConfigurationHelper.loadExecutionConfig(executionName);
		
		if (this.tezClient == null){
			this.createAndTezClient(executionName, fs, tezConfiguration);
		}
		
		TezExecutableDAGBuilder executableDagBuilder = new TezExecutableDAGBuilder(executionName, 
				this.tezClient, this.pipelineConfig);
		
		List<String> outputURIs  = new ArrayList<String>();
		for (int i = 0; i < pipelineExecutionChains.length; i++) {
			ExecutionSpec pipelineExecutionChain = pipelineExecutionChains[i];
			pipelineExecutionChain.getStages().forEach(stage -> executableDagBuilder.addStage(stage, this.getStageParallelizm(stage)) );
			String output = pipelineExecutionChain.getOutputUri() == null 
					? this.tezClient.getClientName() + "/out/"
							: pipelineExecutionChain.getOutputUri().toString();
			output += (pipelineExecutionChains.length > 1 ? "/" + i : "");
			executableDagBuilder.addDataSink(output);
			outputURIs.add(output);
		}
		
		Runnable executable = executableDagBuilder.build();
		
		try {
			executable.run();
			return outputURIs.stream().map(uri -> {
				SequenceFileOutputStreamsBuilder ob = new SequenceFileOutputStreamsBuilder(this.tezClient.getFileSystem(), uri, this.tezClient.getTezConfiguration());
				return Stream.of(ob.build());
			}).collect(Collectors.toList()).toArray(new Stream[]{});
		} 
		catch (Exception e) {
			throw new ExecutionException("Failed to execute DAG for " + executionName, e);
		}
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
	
	/**
	 * 
	 */
	private int getStageParallelizm(Stage stage){
		return this.pipelineConfig.containsKey(DistributableConstants.PARALLELISM + stage.getName()) 
				? Integer.parseInt(this.pipelineConfig.getProperty(DistributableConstants.PARALLELISM + stage.getName())) 
						: 1;
	}
}

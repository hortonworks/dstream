package org.apache.dstream.tez;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DistributableConstants;
import org.apache.dstream.ExecutionDelegate;
import org.apache.dstream.PipelineExecutionChain;
import org.apache.dstream.PipelineExecutionChain.Stage;
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
	public Stream<Stream<?>>[] execute(PipelineExecutionChain... pipelineSpecification) {
		try {
			return this.doExecute(pipelineSpecification);
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
	private Stream<Stream<?>>[] doExecute(PipelineExecutionChain... pipelineSpecification) throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing: " + pipelineSpecification);
		}
		
		TezConfiguration tezConfiguration = new TezConfiguration(new Configuration());
		FileSystem fs;
		try {		
			fs = FileSystem.get(tezConfiguration);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to access FileSystem", e);
		}
		
		this.pipelineConfig = PipelineConfigurationHelper.loadExecutionConfig(pipelineSpecification[0].getJobName());
		
		if (this.tezClient == null){
			this.createAndTezClient(pipelineSpecification[0], fs, tezConfiguration);
		}
		
		TezExecutableDAGBuilder executableDagBuilder = new TezExecutableDAGBuilder(pipelineSpecification[0].getJobName(), 
				this.tezClient, this.pipelineConfig);
		
		AtomicInteger counter = new AtomicInteger();
		List<String> outputURIs = Stream.of(pipelineSpecification)
			.map(pipeline -> {
				pipeline.getStages().forEach(stage -> executableDagBuilder.addStage(stage, this.getStageParallelizm(stage)) );
				String output = pipeline.getOutputUri() == null 
						? this.tezClient.getClientName() + "/out/" + counter.getAndIncrement()
								: pipeline.getOutputUri().toString();
				executableDagBuilder.addDataSink(output);
				return output;
			}).collect(Collectors.toList());
		
		Runnable executable = executableDagBuilder.build();
		
		try {
			executable.run();
			return outputURIs.stream().map(uri -> {
				SequenceFileOutputStreamsBuilder ob = new SequenceFileOutputStreamsBuilder(this.tezClient.getFileSystem(), uri, this.tezClient.getTezConfiguration());
				return Stream.of(ob.build());
			}).collect(Collectors.toList()).toArray(new Stream[]{});
		} 
		catch (Exception e) {
			throw new ExecutionException("Failed to execute DAG for " + pipelineSpecification[0].getJobName(), e);
		}
	}
	
	/**
	 * 
	 * @param pipelineSpecification
	 */
	private void createAndTezClient(PipelineExecutionChain pipelineSpecification, FileSystem fs, TezConfiguration tezConfiguration){	
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, pipelineSpecification.getJobName() + 
												"/" + TezConstants.CLASSPATH_PATH);
		this.tezClient = new ExecutionContextAwareTezClient(pipelineSpecification.getJobName(), 
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

package org.apache.dstream.tez;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.dstream.DistributablePipelineSpecification;
import org.apache.dstream.DistributablePipelineSpecification.Stage;
import org.apache.dstream.ExecutionDelegate;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.tez.utils.HadoopUtils;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
	public Stream<?>[] execute(DistributablePipelineSpecification pipelineSpecification) {
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
	 * @param pipelineSpecification
	 * @return
	 * @throws Exception
	 */
	private Stream<?>[] doExecute(DistributablePipelineSpecification pipelineSpecification) throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing pipeline: " + pipelineSpecification);
		}
		
		TezConfiguration tezConfiguration = new TezConfiguration(new Configuration());
		FileSystem fs;
		try {		
			fs = FileSystem.get(tezConfiguration);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to access FileSystem", e);
		}
		
		this.pipelineConfig = PipelineConfigurationUtils.loadPipelineConfig(pipelineSpecification.getName());
		
		if (this.tezClient == null){
			this.createAndTezClient(pipelineSpecification, fs, tezConfiguration);
		}
		
		List<Stage> stages = pipelineSpecification.getStages();
		TezExecutableDAGBuilder executableDagBuilder = new TezExecutableDAGBuilder(pipelineSpecification.getName(), 
				this.tezClient, this.determineInputFormatClass(stages.get(0)));
	
		stages.stream().forEach(stage -> executableDagBuilder.addStage(stage, this.getStageParallelizm(stage.getId())) );
		
		Callable<Stream<Object>[]> executable = executableDagBuilder.build();
		
		try {
			Stream<?>[] resultStreams = executable.call();
			return resultStreams;
		} 
		catch (Exception e) {
			throw new ExecutionException("Failed to execute DAG for " + pipelineSpecification.getName(), e);
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
	private void createAndTezClient(DistributablePipelineSpecification pipelineSpecification, FileSystem fs, TezConfiguration tezConfiguration){	
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, pipelineSpecification.getName() + 
												"/" + TezConstants.CLASSPATH_PATH);
		this.tezClient = new ExecutionContextAwareTezClient(pipelineSpecification.getName(), 
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
	 * @param firstStage
	 * @return
	 */
	private Class<?> determineInputFormatClass(Stage firstStage){
		SourceSupplier<?> sourceSupplier = firstStage.getSourceSupplier();
		
		if (sourceSupplier.get()[0] instanceof URI){
			String sourceType = this.pipelineConfig.getProperty("source.type", "TXT");
			if (sourceType.equals("TXT")){
				return TextInputFormat.class;
			} 
			else {
				// TODO design a configurable component to handle other standard and custom input types
				throw new IllegalArgumentException("Failed to determine Input Format class for source type " + sourceType);
			}
		} 
		else {
			throw new IllegalArgumentException("Non URI sources are not supported yet");
		}
	}
	
	/**
	 * 
	 * @param stageId
	 * @return
	 */
	private int getStageParallelizm(int stageId){
		if (this.pipelineConfig.containsKey("stage.parallelizm." + stageId)){
			return Integer.parseInt(this.pipelineConfig.getProperty("stage.parallelizm." + stageId));
		}
		return -1;
	}
}

package org.apache.dstream.tez;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.dstream.DistributableConstants;
import org.apache.dstream.PipelineExecutionChain;
import org.apache.dstream.PipelineExecutionChain.Stage;
import org.apache.dstream.ExecutionDelegate;
import org.apache.dstream.support.PipelineConfigurationHelper;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.tez.utils.HadoopUtils;
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
	public Stream<Stream<?>>[] execute(PipelineExecutionChain... pipelineSpecification) {
		try {
			return this.doExecute(pipelineSpecification[0]);
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
	private Stream<Stream<?>>[] doExecute(PipelineExecutionChain... pipelineSpecification) throws Exception {
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
		
		this.pipelineConfig = PipelineConfigurationHelper.loadExecutionConfig(pipelineSpecification[0].getName());
		
		if (this.tezClient == null){
			this.createAndTezClient(pipelineSpecification[0], fs, tezConfiguration);
		}
		
		List<Stage> stages = pipelineSpecification[0].getStages();
		TezExecutableDAGBuilder executableDagBuilder = new TezExecutableDAGBuilder(pipelineSpecification[0].getName(), 
				this.tezClient, this.determineInputFormatClass(stages.get(0)), this.pipelineConfig);
	
		stages.stream().forEach(stage -> executableDagBuilder.addStage(stage, this.getStageParallelizm(stage)) );
		
		Callable<Stream<Object>[]> executable = executableDagBuilder.build();
		
		try {
			Stream<?>[] resultStreams = executable.call();
			return new Stream[]{Stream.of(resultStreams)};
			//return null;
			//return resultStreams;
		} 
		catch (Exception e) {
			throw new ExecutionException("Failed to execute DAG for " + pipelineSpecification[0].getName(), e);
		}
	}
	
	/**
	 * 
	 * @param pipelineSpecification
	 */
	private void createAndTezClient(PipelineExecutionChain pipelineSpecification, FileSystem fs, TezConfiguration tezConfiguration){	
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
	 */
	private Class<?> determineInputFormatClass(Stage firstStage){
		SourceSupplier<?> sourceSupplier = firstStage.getSourceSupplier();
		
		if (sourceSupplier.get()[0] instanceof URI){
			if (firstStage.getSourceItemType().isAssignableFrom(String.class)){
				return TextInputFormat.class;
			} 
			else {
				// TODO design a configurable component to handle other standard and custom input types
				throw new IllegalArgumentException("Failed to determine Input Format class for source item type " + firstStage.getSourceItemType());
			}
		} 
		else {
			throw new IllegalArgumentException("Non URI sources are not supported yet");
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

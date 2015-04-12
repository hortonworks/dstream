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
public class TezPipelineExecutionDelegate {

	private final Logger logger = LoggerFactory.getLogger(TezPipelineExecutionDelegate.class);
	
	private ExecutionContextAwareTezClient tezClient;
	
	private Properties pipelineConfig;
	
	private String sources;
	
	/*
	 * NOTE TO SELF:
	 * While "convention over configuration" exposes a more flexible POJO programming model
	 * there may be a need for an abstract class to help with the flow of things. For example
	 * each delegate must quickly determine if it can process sources provided in pipelineSpecification.
	 * That could be included as a first call in 'execute' method which will delegate to isSourceSupported and then to doExecute. . .
	 */
	public Stream<Object>[] execute(DistributablePipelineSpecification pipelineSpecification) throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing pipeline: " + pipelineSpecification);
		}
		
		this.pipelineConfig = PipelineConfigurationUtils.loadPipelineConfig(pipelineSpecification.getName());
		
		this.sources = this.pipelineConfig.getProperty("source");
		
		if (this.tezClient == null){
			this.createAndTezClient(pipelineSpecification);
		}
		
		List<Stage> stages = pipelineSpecification.getStages();
		TezExecutableDAGBuilder executableDagBuilder = new TezExecutableDAGBuilder(pipelineSpecification.getName(), 
				this.tezClient, this.sources, this.determineInputFormatClass(stages.get(0)));
	
		for (Stage stage : stages) {
			executableDagBuilder.addStage(stage, this.getStageParallelizm(stage.getId()));
		}
		
		Callable<Stream<Object>[]> executable = executableDagBuilder.build();
		
		try {
			Stream<Object>[] resultStreams = executable.call();
			return resultStreams;
		} catch (Exception e) {
			throw new ExecutionException("Failed to execute DAG for " + pipelineSpecification.getName(), e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	protected Credentials getCredentials(){
		return null;
	}
	
	/**
	 * 
	 * @param pipelineSpecification
	 */
	private void createAndTezClient(DistributablePipelineSpecification pipelineSpecification){
		
		TezConfiguration tezConfiguration = new TezConfiguration(new Configuration());
		FileSystem fs;
		try {		
			fs  = FileSystem.get(tezConfiguration);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to access FileSystem", e);
		}
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, pipelineSpecification.getName() + "/" + TezConstants.CLASSPATH_PATH);
		this.tezClient = new ExecutionContextAwareTezClient(pipelineSpecification.getName(), 
											   tezConfiguration, 
											   localResources, 
											   this.getCredentials(),
											   fs);
		try {
			this.tezClient.start();
		} catch (Exception e) {
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
			} else {
				throw new IllegalArgumentException("Failed to determine Input Format class for source type " + sourceType);
			}
		} else {
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

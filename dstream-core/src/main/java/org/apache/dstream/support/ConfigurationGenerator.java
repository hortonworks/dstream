package org.apache.dstream.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.dstream.DistributableConstants;
import org.apache.dstream.DistributableExecutable;
import org.apache.dstream.PipelineExecutionChain.Stage;

public class ConfigurationGenerator {

	private final List<String> sources = new ArrayList<String>();
	
	private final List<String> stageNames = new ArrayList<String>();
	
	private final List<String> stageParalellism = new ArrayList<String>();
	
	private final List<String> msCombine = new ArrayList<String>();
	
	private final DistributableExecutable<?> distributable;
	
	private final StringBuffer buffer = new StringBuffer();
	
	public ConfigurationGenerator(DistributableExecutable<?> distributable) {
		this.distributable = distributable;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String toString(){
		this.processDistributable((List<Stage>)distributable, distributable.getName());

		buffer.append("##### Execution map\n");
		buffer.append("#" + distributable.toString().replace(", ", " -> ") + "\n\n");
		this.stageNames.forEach(buffer::append);
		buffer.append("\n");
		
		buffer.append("# ==== REQUIRED FRAMEWORK PROPERTIES ====\n");
		buffer.append("# \"dstream.source.{pipelineName}\" - defines the source of the pipeline.\n");
		buffer.append("# Values could either be valid URIs (delimited by \";\") or fully qualified name of\n");
		buffer.append("# the class that implements org.apache.dstream.support.SourceSupplier.\n");
		buffer.append("# Valid URI must begin with scheme (e.g., \"file:\", \"http:\")\n");
		buffer.append("# Values can also contain references to system properties. For example:\n");
		buffer.append("# dstream.source.foo=file:${user.dir}/sample/foo.txt; file:${user.dir}/sample/bar.txt\n");
		this.sources.forEach(buffer::append);
		buffer.append("\n");
		
		buffer.append("# ==== OPTIONAL FRAMEWORK PROPERTIES ====\n");
		buffer.append("#dstream.output=\n");
		buffer.append("\n");
		this.stageParalellism.forEach(buffer::append);
		buffer.append("\n");
		this.msCombine.forEach(buffer::append);
		
		return buffer.toString();
	}
	
	private void processDistributable(List<Stage> stages, String name){
		this.addSource(name);
		stages.forEach(stage -> {
			this.addStage(stage);
			this.addStageParallelizm(stage);
			this.addStageMsCombine(stage);
			if (stage.getDependentExecutionContextSpec() != null){
				this.processDistributable(stage.getDependentExecutionContextSpec().getStages(), stage.getDependentExecutionContextSpec().getJobName());
			}
		});
	}
	
	
	private void addSource(String source){
		this.sources.add("dstream.source." + source + "=\n");
	}
	
	private void addStage(Stage stage){
		this.stageNames.add("#" + stage.getName() + "=" + stage.getOperationNames() + "\n");
	}
	
	private void addStageParallelizm(Stage stage){
		this.stageParalellism.add("#" + DistributableConstants.PARALLELISM  + stage.getName() + "=" + "\n");
	}
	
	private void addStageMsCombine(Stage stage){
		this.msCombine.add("#" + DistributableConstants.MAP_SIDE_COMBINE + stage.getName() + "=" + "\n");
	}
}

package org.apache.dstream.assembly;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.io.StreamableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 */
public class StreamAssembly implements Iterable<Stage>{

	private final Logger logger = LoggerFactory.getLogger(StreamAssembly.class);
	
	private volatile String jobName;
	
	private volatile List<Stage> stages;
	
	private volatile StreamableSource<?> source;
	
	private volatile OutputSpecification outputSpecification;

	private StreamAssembly(){
		this.stages = new ArrayList<Stage>();
	}
	
	public StreamableSource<?> getSource() {
		return source;
	}

	public String getJobName() {
		return jobName;
	}
	
	public void addStage(Stage stage){
		this.stages.add(stage);
	}
	
	public Stage getLastStage(){
		return stages.get(stages.size()-1);
	}

	@Override
	public Iterator<Stage> iterator() {
		return this.stages.iterator();
	}
	
	public int getStageCount(){
		return stages.size();
	}
	
	public OutputSpecification getOutputSpecification() {
		return outputSpecification;
	}

	public void setOutputSpecification(OutputSpecification outputSpecification) {
		this.outputSpecification = outputSpecification;
	}
}

package org.apache.dstream.assembly;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.transform.stream.StreamSource;

import org.apache.dstream.DistributableSource;
import org.apache.dstream.OutputSpecification;


/**
 * 
 */
public class StreamAssembly<T> implements Iterable<Stage<T>>{

	private volatile String jobName;
	
	private volatile List<Stage<T>> stages;
	
	private volatile DistributableSource<?> source;
	
	private volatile OutputSpecification outputSpecification;

	private StreamAssembly(){
		this.stages = new ArrayList<Stage<T>>();
	}
	
	public DistributableSource<?> getSource() {
		return source;
	}

	public String getJobName() {
		return jobName;
	}
	
	public void addStage(Stage<T> stage){
		this.stages.add(stage);
	}
	
	public Stage<T> getLastStage(){
		return stages.get(stages.size()-1);
	}

	@Override
	public Iterator<Stage<T>> iterator() {
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
	
	public String toString(){
		return this.jobName + " - Stages:[" + stages.size() + "]";
	}
}

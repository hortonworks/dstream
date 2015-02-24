package org.apache.dstream.dag;

import java.util.ArrayList;
import java.util.List;

import org.apache.dstream.io.StreamableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 */
public class DagContext {

	private final Logger logger = LoggerFactory.getLogger(DagContext.class);
	
	private volatile String jobName;
	
	private volatile List<Stage> stages;
	
	private volatile StreamableSource<?> source;

	private DagContext(){
		this.stages = new ArrayList<Stage>();
	}

	public String getJobName() {
		return jobName;
	}
	
	public void addStage(Stage stage){
		this.stages.add(stage);
	}
}

package org.apache.dstream.assembly;

import java.io.Serializable;

import org.apache.dstream.Merger;
import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 */
public class Stage implements Serializable {
	private static final long serialVersionUID = 5499538870738016508L;
	
	private final SerializableFunction<?, ?> stageFunction;

	private volatile Merger<?,?> merger;

	/**
	 * 
	 * @param stageFunction
	 */
	public Stage(SerializableFunction<?, ?> stageFunction){
		this.stageFunction = stageFunction;
	}
	
	public void setMerger(Merger<?, ?> merger) {
		this.merger = merger;
	}
	
	public Merger<?, ?> getMerger() {
		return merger;
	}
	
	public SerializableFunction<?, ?> getStageFunction() {
		return stageFunction;
	}
}

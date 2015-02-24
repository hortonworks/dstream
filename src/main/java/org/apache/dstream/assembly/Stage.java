package org.apache.dstream.assembly;

import java.io.Serializable;
import java.util.stream.Stream;

import org.apache.dstream.Merger;
import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 */
public class Stage<T,R> implements Serializable {
	private static final long serialVersionUID = 5499538870738016508L;
	
	private final SerializableFunction<Stream<T>,R> stageFunction;

	private volatile Merger<?,?> merger;

	/**
	 * 
	 * @param stageFunction
	 */
	public Stage(SerializableFunction<Stream<T>,R> stageFunction){
		this.stageFunction = stageFunction;
	}
	
	public void setMerger(Merger<?, ?> merger) {
		this.merger = merger;
	}
	
	public Merger<?, ?> getMerger() {
		return merger;
	}
	
	public SerializableFunction<Stream<T>,R> getStageFunction() {
		return stageFunction;
	}
}

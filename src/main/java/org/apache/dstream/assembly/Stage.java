package org.apache.dstream.assembly;

import java.io.Serializable;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateResult;
import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 */
public class Stage<T> implements Serializable {
	private static final long serialVersionUID = 5499538870738016508L;
	
	private final SerializableFunction<Stream<T>,?> stageFunction;
	
	private final SerializableFunction<Stream<?>,Stream<?>> preProcessFunction;

	private volatile IntermediateResult<?,?> merger;
	
	private final int stageId;

	/**
	 * 
	 * @param stageFunction
	 */
	public Stage(SerializableFunction<Stream<T>,?> stageFunction, SerializableFunction<Stream<?>,Stream<?>> preProcessFunction, int stageId){
		this.stageFunction = stageFunction;
		this.stageId = stageId;
		this.preProcessFunction = preProcessFunction;
	}
	
	public void setMerger(IntermediateResult<?,?> merger) {
		this.merger = merger;
	}
	
	public IntermediateResult<?,?> getMerger() {
		return merger;
	}
	
	public SerializableFunction<Stream<T>,?> getStageFunction() {
		return stageFunction;
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public SerializableFunction<Stream<?>, Stream<?>> getPreProcessFunction() {
		return preProcessFunction;
	}
}

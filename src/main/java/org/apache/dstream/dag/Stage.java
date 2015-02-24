package org.apache.dstream.dag;

import java.io.Serializable;

import org.apache.dstream.utils.SerializableFunction;

public class Stage implements Serializable {
	private static final long serialVersionUID = 5499538870738016508L;
	
	private final SerializableFunction<?, ?> stageFunction;

	public Stage(SerializableFunction<?, ?> stageFunction){
		this.stageFunction = stageFunction;
	}
}

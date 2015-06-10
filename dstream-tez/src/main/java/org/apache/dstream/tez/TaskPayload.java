package org.apache.dstream.tez;

import java.io.Serializable;
import java.util.stream.Stream;

import org.apache.dstream.support.Parallelizer;
import org.apache.dstream.support.SerializableFunctionConverters.Function;

class TaskPayload implements Serializable {
	private static final long serialVersionUID = 8560690101888115352L;

	private final Function<Stream<?>, Stream<?>> task;

	private Parallelizer<? super Object> parallelizer;

	TaskPayload(Function<Stream<?>, Stream<?>> task){
		this.task = task;
	}
	
	Parallelizer<? super Object> getParallelizer() {
		return parallelizer;
	}

	void setParallelizer(Parallelizer<? super Object> parallelizer) {
		this.parallelizer = parallelizer;
	}
	
	Function<Stream<?>, Stream<?>> getTask() {
		return task;
	}
}

package org.apache.dstream.tez;

import java.io.Serializable;
import java.util.stream.Stream;

import org.apache.dstream.Splitter;
import org.apache.dstream.support.SerializableFunctionConverters.Function;

class TaskPayload implements Serializable {
	private static final long serialVersionUID = 8560690101888115352L;

	private final Function<Stream<?>, Stream<?>> task;

	private Splitter<? super Object> splitter;

	TaskPayload(Function<Stream<?>, Stream<?>> task){
		this.task = task;
	}
	
	Splitter<? super Object> getSplitter() {
		return splitter;
	}

	void setSplitter(Splitter<? super Object> splitter) {
		this.splitter = splitter;
	}
	
	Function<Stream<?>, Stream<?>> getTask() {
		return task;
	}
}

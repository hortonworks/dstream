package org.apache.dstream.exec;

import java.util.stream.Stream;

import org.apache.dstream.assembly.StreamAssembly;

public abstract class StreamExecutor<T> {

	protected final StreamAssembly streamAssembly;
	
	public StreamExecutor(StreamAssembly streamAssembly){
		this.streamAssembly = streamAssembly;
	}
	
	public abstract Stream<T> execute();
}

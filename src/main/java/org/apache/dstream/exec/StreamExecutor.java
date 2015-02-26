package org.apache.dstream.exec;

import java.util.stream.Stream;

import org.apache.dstream.assembly.StreamAssembly;

public abstract class StreamExecutor<T,R> {

	protected final StreamAssembly<T> streamAssembly;
	
	public StreamExecutor(StreamAssembly<T> streamAssembly){
		this.streamAssembly = streamAssembly;
	}
	
	public abstract Stream<R> execute();
}

package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableFunction;

public interface StreamSource<T> {
	
	public abstract Stream<T> toStream();
	
	/**
	 * Function that will be appended to the main pipeline
	 * @return
	 */
	public abstract SerializableFunction<Stream<?>, Stream<?>> getPreprocessFunction();
	
	public abstract void setPreprocessFunction(SerializableFunction<Stream<?>, Stream<?>> preProcessFunction);
	
	public abstract StreamSource<T> preProcessSource(SerializableFunction<Path[], Path[]> sourcePreProcessFunction);
	
}

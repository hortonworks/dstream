package org.apache.dstream;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 * @param <T>
 */
public abstract class AbstractDistributableSource<T> implements DistributableSource<T> {
	
	private SerializableFunction<Stream<?>, Stream<?>> preProcessFunction;
	
	protected SerializableFunction<Stream<?>, Stream<?>> getPreprocessFunction() {
		return this.preProcessFunction;
	}
	
	protected void setPreprocessFunction(SerializableFunction<Stream<?>, Stream<?>> preProcessFunction){
		this.preProcessFunction = preProcessFunction;
	}
	
	/**
	 * Allows source to be pre-processed to render final {@link DistributableSource}. An example of such pre-processing may be 
	 * additional file filtering
	 * 
	 * @param sourcePreProcessFunction
	 * @return
	 */
	protected abstract DistributableSource<T> preProcessSource(SerializableFunction<Path[], Path[]> sourcePreProcessFunction);
	
}

package org.apache.dstream;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 * @param <T>
 */
public abstract class AbstractDistributedPipeline<T> implements DataPipeline<T> {
	
	private SerializableFunction<Stream<?>, Stream<?>> preProcessFunction;
	
	protected SerializableFunction<Stream<?>, Stream<?>> getPreprocessFunction() {
		return this.preProcessFunction;
	}
	
	protected void setPreprocessFunction(SerializableFunction<Stream<?>, Stream<?>> preProcessFunction){
		this.preProcessFunction = preProcessFunction;
	}
	
	/**
	 * Allows source to be pre-processed to render final {@link DataPipeline}. An example of such pre-processing may be 
	 * additional file filtering
	 * 
	 * @param sourcePreProcessFunction
	 * @return
	 */
	protected abstract DataPipeline<T> preProcessSource(SerializableFunction<Path[], Path[]> sourcePreProcessFunction);
	
}

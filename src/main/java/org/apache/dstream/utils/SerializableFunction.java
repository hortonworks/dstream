package org.apache.dstream.utils;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Simple {@link Serializable} wrapper over {@link Function}
 * 
 * @param <T>
 * @param <R>
 */
public interface SerializableFunction<T,R> extends Function<T, R>, Serializable {
	
}

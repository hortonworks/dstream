package org.apache.dstream.utils;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Simple {@link Serializable} wrapper over {@link Function}
 * 
 * @param <T>
 * @param <R>
 */
public interface SerializableFunction<T,R> extends Function<T, R>, Serializable {
	
	default <V> Function<V, R> compose(Function<? super V, ? extends T> before) {
        Objects.requireNonNull(before);
        return (V v) -> apply(before.apply(v));
    }
	
}

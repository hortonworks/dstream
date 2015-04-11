package org.apache.dstream;

import java.io.Serializable;
import java.util.Objects;
/**
 * Defines {@link Serializable} equivalents of relevant JDK strategies (e.g., {@link java.util.function.Function})
 *
 */
public final class SerializableHelpers {
	
	public static interface Function<T,R> extends java.util.function.Function<T, R>, Serializable{
		@Override
		default <V> Function<V, R> compose(java.util.function.Function<? super V, ? extends T> before) {
	        Objects.requireNonNull(before);
	        return new Function<V, R>() {
				private static final long serialVersionUID = -8429315342325486066L;
				@Override
				public R apply(V t) {
					return (R) Function.this.apply((T) before.apply(t));
				}	
	        };
	    }
			
		@Override
		default <V> Function<T, V> andThen(java.util.function.Function<? super R, ? extends V> after) {
	        Objects.requireNonNull(after);
	        return new Function<T,V>() {
				private static final long serialVersionUID = -559880927330709790L;
				@Override
				public V apply(T t) {
					return after.apply(Function.this.apply(t));
				}	
	        };
	    }
	}
	
	public static interface BinaryOperator<T> extends java.util.function.BinaryOperator<T>, Serializable {}
	
	public static interface Supplier<T> extends java.util.function.Supplier<T>, Serializable{}
	
	public static interface BiFunction<T, U, R> extends java.util.function.BiFunction<T, U, R>, Serializable{}
	
	public static interface Predicate<T> extends java.util.function.Predicate<T>, Serializable{}
}

package org.apache.dstream.support;

import java.io.Serializable;
import java.util.Objects;
/**
 * Defines {@link Serializable} equivalents strategies defined in java.util.function package.
 */
public final class SerializableFunctionConverters {
	
	/**
	 * {@link Serializable} version of {@link java.util.function.Function}
	 * 
	 * @param <T>
	 * @param <R>
	 */
	public static interface Function<T,R> extends java.util.function.Function<T, R>, Serializable{
		default <V> Function<V, R> compose(Function<? super V, ? extends T> before) {
	        Objects.requireNonNull(before);
	        return new Function<V, R>() {
				private static final long serialVersionUID = -8429315342325486066L;
				@Override
				public R apply(V t) {
					return (R) Function.this.apply((T) before.apply(t));
				}	
	        };
	    }
			
		default <V> Function<T, V> andThen(Function<? super R, ? extends V> after) {
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
	
	/**
	 * {@link Serializable} version of {@link java.util.function.BinaryOperator}
	 * 
	 * @param <T>
	 */
	public static interface BinaryOperator<T> extends java.util.function.BinaryOperator<T>, Serializable {}
	
	/**
	 * {@link Serializable} version of {@link java.util.function.Supplier}
	 * 
	 * @param <T>
	 */
	public static interface Supplier<T> extends java.util.function.Supplier<T>, Serializable{}
	
	/**
	 * {@link Serializable} version of {@link java.util.function.BiFunction}
	 * 
	 * @param <T>
	 * @param <U>
	 * @param <R>
	 */
	public static interface BiFunction<T, U, R> extends java.util.function.BiFunction<T, U, R>, Serializable{}
	
	/**
	 * {@link Serializable} version of {@link java.util.function.Predicate}
	 * 
	 * @param <T>
	 */
	public static interface Predicate<T> extends java.util.function.Predicate<T>, Serializable{}
}

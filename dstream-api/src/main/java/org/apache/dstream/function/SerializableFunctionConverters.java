package org.apache.dstream.function;

import java.io.Serializable;
import java.util.Objects;
/**
 * Defines {@link Serializable} equivalents to strategies defined in java.util.function package
 * that are used by this framework.
 */
public interface SerializableFunctionConverters {
	
	/**
	 * {@link Serializable} version of {@link java.util.function.Function}
	 */
	public static interface SerFunction<T,R> extends java.util.function.Function<T, R>, Serializable{
		default <V> SerFunction<V, R> compose(SerFunction<? super V, ? extends T> before) {
	        Objects.requireNonNull(before);
	        return new SerFunction<V, R>() {
				private static final long serialVersionUID = -8429315342325486066L;
				@Override
				public R apply(V t) {
					return (R) SerFunction.this.apply((T) before.apply(t));
				}	
	        };
	    }
			
		default <V> SerFunction<T, V> andThen(SerFunction<? super R, ? extends V> after) {
	        Objects.requireNonNull(after);
	        return new SerFunction<T,V>() {
				private static final long serialVersionUID = -559880927330709790L;
				@Override
				public V apply(T t) {
					return after.apply(SerFunction.this.apply(t));
				}	
	        };
	    }
	}
	
	/**
	 * {@link Serializable} version of {@link java.util.function.BinaryOperator}
	 */
	public static interface SerBinaryOperator<T> extends java.util.function.BinaryOperator<T>, Serializable {}
	
	/**
	 * {@link Serializable} version of {@link java.util.function.Supplier}
	 */
	public static interface SerSupplier<T> extends java.util.function.Supplier<T>, Serializable{}
	
	/**
	 * {@link Serializable} version of {@link java.util.function.BiFunction}
	 */
	public static interface SerBiFunction<T, U, R> extends java.util.function.BiFunction<T, U, R>, Serializable{}
	
	/**
	 * {@link Serializable} version of {@link java.util.function.Predicate}
	 */
	public static interface SerPredicate<T> extends java.util.function.Predicate<T>, Serializable{}
}

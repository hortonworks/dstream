package org.apache.dstream;

import java.io.Serializable;

public final class SerializableLambdas {
	
	public static interface Function<T,R> extends java.util.function.Function<T, R>, Serializable{}
	
	public static interface BinaryOperator<T> extends java.util.function.BinaryOperator<T>, Serializable{}

}

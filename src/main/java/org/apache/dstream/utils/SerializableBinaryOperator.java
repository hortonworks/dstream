package org.apache.dstream.utils;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public interface SerializableBinaryOperator<T> extends BinaryOperator<T>, BiFunction<T,T,T>, Serializable {

}

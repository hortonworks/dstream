package org.apache.dstream.utils;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBinaryOperator<T> extends  BiFunction<T,T,T>, Serializable {

}

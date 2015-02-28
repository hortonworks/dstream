package org.apache.dstream.utils;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<A,B,R> extends BiFunction<A,B,R>, Serializable {

}

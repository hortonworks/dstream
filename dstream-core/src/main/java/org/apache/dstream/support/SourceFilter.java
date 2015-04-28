package org.apache.dstream.support;

public interface SourceFilter<T> {

	boolean accept(T source);
}

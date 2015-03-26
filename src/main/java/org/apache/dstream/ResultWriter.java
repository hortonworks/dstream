package org.apache.dstream;

public interface ResultWriter<T> {

	void write(T result);
}

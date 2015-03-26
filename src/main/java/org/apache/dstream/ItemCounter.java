package org.apache.dstream;

public interface ItemCounter<T> {

	Number count(Object reader);
}

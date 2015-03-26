package org.apache.dstream;
/**
 * Generic strategy to provide strategy to implement Reader/Writer centric data processors.
 *
 * @param <R> - return type
 * @param <T> - source type
 * @param <V> - result type
 */
public interface Processor<R,T,V> {

	R process(ItemReader<T> reader, ResultWriter<V> writer);
}

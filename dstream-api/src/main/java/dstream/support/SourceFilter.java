package dstream.support;

/**
 * 
 * @param <T>
 */
public interface SourceFilter<T> {

	boolean accept(T source);
}

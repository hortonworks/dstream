package dstream.support;

import java.io.Serializable;

/**
 * Implementation of {@link SharedReference} which holds the value 
 * as an instance variable.
 * 
 * @param <T> the type of the value. Must be {@link Serializable}.
 */
public class SimpleSharedReference<T extends Serializable> extends SharedReference<T> {
	private static final long serialVersionUID = 431225770952426624L;

	private T value; 

	@Override
	protected void set(T value) {
		this.value = value;
	}

	@Override
	public T get() {
		return this.value;
	}
}
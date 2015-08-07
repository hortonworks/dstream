package dstream.support;

import java.io.Serializable;

import dstream.support.SharedReference.MutableSharedReference;


public class TestSharedReference<T extends Serializable> extends MutableSharedReference<T> {
	private static final long serialVersionUID = 638733002642791647L;
	
	private T object;
	
	@Override
	public T get() {
		return this.object;
	}

	@Override
	protected void set(T object) {
		this.object = object;
	}

}

package org.apache.dstream.tez;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.function.StreamUnionFunction;

public class TezUnionFunction extends StreamUnionFunction{
	private static final long serialVersionUID = -6486755428802968586L;

	public TezUnionFunction(boolean unionAll) {
		super(unionAll);
	}
	
	protected Stream<?> preProcessStream(Stream<?> stream) {
		return KeyValuesNormalizer.normalize((Stream<Entry<Object, Iterator<Object>>>) stream);
	}
}

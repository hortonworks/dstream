package org.apache.dstream.tez;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;

import dstream.function.StreamJoinerFunction;

class TezJoiner extends StreamJoinerFunction {
	private static final long serialVersionUID = -2554454163443511159L;
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Stream<?> preProcessStream(Stream<?> stream) {
		return KeyValuesNormalizer.normalize((Stream<Entry<Object, Iterator<Object>>>) stream);
	}
}

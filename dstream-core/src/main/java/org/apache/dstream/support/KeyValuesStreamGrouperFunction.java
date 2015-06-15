package org.apache.dstream.support;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;

/**
 * An extension to {@link KeyValuesStreamCombinerFunction} which simply ensures the 
 * semantical correctness of the value (Iterable[V]). In a typical case where more then one
 * value existed in the first place it is the expectation that the <i>combiner</i> will 
 * produce an {@link Iterable}. However, for cases where there was only one value, <i>combiner</i> 
 * is never called, so this function ensures that such single value is wrapped into {@link Iterable}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KeyValuesStreamGrouperFunction<K,V,T> extends  KeyValuesStreamCombinerFunction<K, V, T> {
	private static final long serialVersionUID = 5774838692658472433L;
	
	@SuppressWarnings("rawtypes")
	public KeyValuesStreamGrouperFunction(BinaryOperator combiner) {
		super(combiner);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected Object buildValue(Stream<V> valuesStream){
		Object value = super.buildValue(valuesStream);
		if (!(value instanceof Iterable)){
			List<V> l = new ArrayList<V>();
			l.add((V) value);
			value = l;
		}
		return value;
	}
}

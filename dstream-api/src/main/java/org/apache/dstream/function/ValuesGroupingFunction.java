package org.apache.dstream.function;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.SerBinaryOperator;

/**
 * An extension to {@link ValuesReducingFunction} which simply ensures the 
 * semantical correctness of the value (Iterable[V]). In a typical case where more then one
 * value existed in the first place it is the expectation that the <i>combiner</i> will 
 * produce an {@link Iterable}. However, for cases where there was only one value, <i>combiner</i> 
 * is never called, so this function ensures that such single value is wrapped into {@link Iterable}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ValuesGroupingFunction<K,V,T> extends  ValuesReducingFunction<K, V, T> {
	private static final long serialVersionUID = 5774838692658472433L;
	
	@SuppressWarnings("rawtypes")
	public ValuesGroupingFunction(SerBinaryOperator combiner) {
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

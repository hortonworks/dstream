package dstream.local.ri;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import dstream.support.Aggregators;

public class PartitionGrouper {
	
	protected static class RefHolder {
		public final Object ref;
		public RefHolder(Object ref){
			this.ref = ref;
		}
		@Override
		public String toString(){
			return this.ref.toString();
		}
		
		@Override
		public int hashCode(){
			return this.ref.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			return obj instanceof RefHolder && this.ref.equals(((RefHolder)obj).ref);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T group(Object v1, Object v2) {
		RefHolder refHolder = (RefHolder) v2;
		v2 = refHolder.ref;
		
		T aggregatedValues;
		if (v2 instanceof Entry){
			aggregatedValues = (T) toMap(v1);
			Entry<Object, Object> entry = (Entry<Object, Object>) v2;
			((Map<Object, Object>)aggregatedValues).merge(entry.getKey(), entry.getValue(), Aggregators::aggregateFlatten);
		}
		else {
			aggregatedValues = (T)toList(v1);
			((List)aggregatedValues).add(v2);
		}
		
		return aggregatedValues;
	}
	
	private static <K,V> Map<K,V> toMap(Object value) {
		if (value instanceof RefHolder){
			value = ((RefHolder)value).ref;
		}
		Map<K,V> aggregatedValues;
		if (value instanceof Map){
			aggregatedValues = (Map<K,V>) value;
		}
		else {
			aggregatedValues = new HashMap<>();
			Entry<K,V> entry = (Entry<K,V>) value;
			aggregatedValues.put(entry.getKey(), entry.getValue());
		}
		return aggregatedValues;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private static <T> List<T> toList(Object value){
		if (value instanceof RefHolder){
			value = ((RefHolder)value).ref;
		}
		List<T> aggregatedValues;
		if (value instanceof List){
			aggregatedValues = (List<T>)value;
		}
		else {
			aggregatedValues = new ArrayList<T>();
			aggregatedValues.add((T) value);
		}
		return aggregatedValues;
	}
}

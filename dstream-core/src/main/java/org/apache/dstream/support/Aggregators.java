package org.apache.dstream.support;

import java.util.ArrayList;
import java.util.List;

public interface Aggregators {

	@SuppressWarnings("unchecked")
	public static <T> List<T> aggregate(Object v1, T v2) {
		List<T> aggregatedValues;
		if (v1 instanceof List){
			aggregatedValues = (List<T>)v1;
		}
		else {
			aggregatedValues = new ArrayList<T>();
			aggregatedValues.add((T) v1);
		}
		aggregatedValues.add(v2);
		return aggregatedValues;
	}
}

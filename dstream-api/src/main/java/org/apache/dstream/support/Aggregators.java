package org.apache.dstream.support;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.dstream.function.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.utils.Assert;

/**
 * Strategy which provides implementations of common aggregation functionality 
 *
 */
public abstract class Aggregators {
	
	/**
	 * Aggregation operation which collects single (non-{@link List}) values into a {@link List}.<br>
	 * If left value (v1) is not {@link List}, the new (accumulating) {@link List} will be created and 
	 * left value added to it, otherwise left value is treated as accumulating {@link List}.
	 * The right value (v2) is then added to the accumulating list following this rule:<br>
	 * If right value (v2) is of type {@link List} the {@link IllegalArgumentException} is thrown, otherwise the 
	 * value is added to the accumulating list<br>
	 * <br>
	 * It could be used as {@link BiFunction} or {@link BinaryOperator} (e.g., Aggregators::aggregateSingleObjects)
	 * 
	 * @param v1 first value which on each subsequent invocation is of type {@link List}
	 * @param v2 second value which must not be of type {@link List}
	 * @return aggregated values as {@link List}
	 */
	public static <T> List<T> aggregateSingleObjects(Object v1, T v2) {
		Assert.isFalse(v2 instanceof List, "'v2' must not be a List when using this operation");
		List<T> aggregatedValues = toList(v1);
		aggregatedValues.add(v2);
		return aggregatedValues;
	}
	
	/**
	 * Aggregation operation which collects values into a {@link List}.<br>
	 * If left value (v1) is not {@link List}, the new (accumulating) {@link List} will be created and 
	 * left value added to it, otherwise left value is treated as accumulating {@link List}.
	 * The right value (v2) is then added to the accumulating list following this rule:<br>
	 * If right value (v2) is of type {@link List} its contents will be added (as {@link List#addAll(java.util.Collection)}) 
	 * to the accumulating list essentially flattening the structure, otherwise single value is added to 
	 * the accumulating list<br>
	 * See {@link #aggregateSingleObjects(Object, Object)} when collected values are never {@link List}.<br>
	 * <br>
	 * It could be used as {@link BiFunction} or {@link BinaryOperator} (e.g., Aggregators::aggregateFlatten)
	 * 
	 * @param v1 first value which on each subsequent invocation is of type {@link List}
	 * @param v2 second value which could be single value or of type {@link List}
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> List<T> aggregateFlatten(Object v1, T v2) {
		List<Object> aggregatedValues = toList(v1);
		if (v2 instanceof List){
			aggregatedValues.addAll((List<T>)v2);
		}
		else {
			aggregatedValues.add(v2);
		}
		return (List<T>) aggregatedValues;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private static <T> List<T> toList(Object v1){
		List<T> aggregatedValues;
		if (v1 instanceof List){
			aggregatedValues = (List<T>)v1;
		}
		else {
			aggregatedValues = new ArrayList<T>();
			aggregatedValues.add((T) v1);
		}
		return aggregatedValues;
	}
}

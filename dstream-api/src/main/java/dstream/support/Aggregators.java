/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream.support;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import dstream.function.SerializableFunctionConverters.SerBinaryOperator;

/**
 * Strategy which provides implementations of common aggregation functionality 
 *
 */
public abstract class Aggregators {
	
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
	 * It could be used as {@link BiFunction} or {@link SerBinaryOperator} (e.g., Aggregators::aggregateFlatten)
	 * 
	 * @param v1 first value which on each subsequent invocation is of type {@link List}
	 * @param v2 second value which could be single value or of type {@link List}
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> List<T> aggregateToList(Object v1, T v2) {
		List<Object> aggregatedValues = toList(v1);
		aggregatedValues.add(v2);
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

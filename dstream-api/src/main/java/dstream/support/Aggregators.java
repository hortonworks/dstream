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

/**
 * Strategy which provides implementations of common aggregation functionality
 *
 */
public abstract class Aggregators {

	/**
	 * Aggregation operation which collects values into a {@link List}.
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

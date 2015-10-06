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
package io.dstream;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Defines all available operations.
 */
public enum Ops {
	aggregateValues, 
	classify,
	compute,
	count,
	distinct,
	extract,
	filter,
	flatMap,
	join,
	load,
	map,
	mapKeyValues,
	max, 
	min,
	on,
	reduce,
	reduceValues,
	sorted,
	peek,
	union,
	unionAll;
	
	/**
	 * Returns <i>true</i> if the operation identified by the given name is 
	 * a <i>transformation</i> operation. 
	 */
	public static boolean isTransformation(String operationName){
		return isTransformation(Ops.valueOf(operationName));
	}
	
	/**
	 * Returns <i>true</i> if the operation identified by the given name is 
	 * a <i>shuffle</i> operation. 
	 */
	public static boolean isShuffle(String operationName){
		return isShuffle(Ops.valueOf(operationName));
	}
	
	/**
	 * Returns <i>true</i> if the operation identified by the given name is 
	 * compatible with <i>reduce</i> operations from {@link Stream}. 
	 */
	public static boolean isStreamReduce(String operationName){
		return isStreamReduce(Ops.valueOf(operationName));
	}
	
	/**
	 * Returns <i>true</i> if the operation identified by the given name is 
	 * compatible with {@link Stream} operations that take {@link Comparator} as an argument.
	 */
	public static boolean isStreamComparator(String operationName){
		return isStreamComparator(Ops.valueOf(operationName));
	}

	public static boolean isStreamConsumer(String operationName) {
		return isStreamConsumer(Ops.valueOf(operationName));
	}
	
	/**
	 * Returns <i>true</i> if the operation identified by the given {@link Ops} is 
	 * compatible with {@link Stream} operations that take {@link Comparator} as an argument.
	 */
	public static boolean isStreamComparator(Ops operation){
		return operation.equals(min) ||
			   operation.equals(max) ||
			   operation.equals(distinct) ||
			   operation.equals(sorted);
	}

	public static boolean isStreamConsumer(Ops operation) {
		return operation.equals(peek);
	}

	
	/**
	 * Returns <i>true</i> if the operation identified by the given {@link Ops} is 
	 * compatible with <i>reduce</i> operations from {@link Stream}. 
	 */
	public static boolean isStreamReduce(Ops operation){
		return operation.equals(count) ||
			   operation.equals(reduce);
	}
	
	/**
	 * Returns <i>true</i> if the operation identified by the given {@link Ops} is 
	 * a <i>transformation</i> operation. 
	 */
	public static boolean isTransformation(Ops operation){
		return operation.equals(flatMap) ||
			   operation.equals(map) ||
			   operation.equals(filter) ||
			   operation.equals(compute);
	}
	
	/**
	 * Returns <i>true</i> if the operation identified by the given {@link Ops} is 
	 * a <i>shuffle</i> operation. 
	 */
	public static  boolean isShuffle(Ops operation){
		return operation.equals(reduceValues) ||
			   operation.equals(aggregateValues) ||
			   operation.equals(join) ||
			   operation.equals(union) ||
			   operation.equals(unionAll) ||
			   operation.equals(classify);
	}
}

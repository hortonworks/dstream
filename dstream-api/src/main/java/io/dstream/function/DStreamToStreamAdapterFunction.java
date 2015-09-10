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
package io.dstream.function;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.dstream.DStream;
import io.dstream.Ops;
import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.SerializableStreamAssets.SerPredicate;
import io.dstream.utils.Assert;
import io.dstream.utils.ReflectionUtils;

/**
 * An implementation of {@link SerFunction} which will translate Stream-like
 * invocations on the {@link DStream} to {@link Stream} operations.
 * It is created for each invocation of operations that match operations defined on {@link Stream}.
 * At the time of writing this javadoc these operations are - <i>map, flatMap and filter.</i>
 */
public class DStreamToStreamAdapterFunction implements SerFunction<Stream<?>, Stream<?>>{
	private static final long serialVersionUID = 6836233233261184905L;
	
	private static final Map<Ops, Method> supportedOperations = buildSupportedOperations(Stream.of(
			Ops.flatMap, 
			Ops.map, 
			Ops.filter, 
			Ops.distinct,
			Ops.min,
			Ops.max,
			Ops.sorted));
	
	private final String streamOperationName;
	
	private final Object streamOperation;
	
	/**
	 * Constructs this function.
	 * 
	 * @param streamOperationName the name of the target operation.
	 * @param streamOperation target operation. The reason why it's an Object is because it could be 
	 * 		{@link SerFunction} or {@link SerPredicate}.
	 */
	public DStreamToStreamAdapterFunction(String streamOperationName, Object streamOperation){
		Assert.notEmpty(streamOperationName, "'streamOperationName' must not be null or empty");

		if (!supportedOperations.containsKey(Ops.valueOf(streamOperationName))){
			throw new IllegalArgumentException("Operation '" + streamOperationName + "' is not supported");
		}
		
		this.streamOperation = streamOperation;
		this.streamOperationName = streamOperationName;
	}

	/**
	 * 
	 */
	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		try {
			Method m = supportedOperations.get(Ops.valueOf(this.streamOperationName));
			Stream<?> result;
			if (m.getName().equals(Ops.distinct.name())){
				result = ((Stream<?>) m.invoke(streamIn));
			}
			else {
				Object tRes = m.invoke(streamIn, this.streamOperation);
				// min/max will have Optional result
				result = tRes instanceof Optional ? Stream.of(((Optional<?>)tRes).get()) : (Stream<?>) tRes;
			}
			return result;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Operation '" + this.streamOperationName + "' is not supported.", e);
		}
	}
	
	/**
	 * 
	 */
	private static Map<Ops, Method> buildSupportedOperations(Stream<Ops> operationsStream){
		return operationsStream
				.collect(Collectors.toMap(op -> op, op -> {
					if (op.equals(Ops.sorted)){
						return ReflectionUtils.findMethod(Ops.sorted.name(), Stream.class, Stream.class, Comparator.class);
					}
					else {
						return ReflectionUtils.findSingleMethod(op.name(), Stream.class);
					}
				} ));
	}
}

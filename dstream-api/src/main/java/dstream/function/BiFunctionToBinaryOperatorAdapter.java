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
package dstream.function;

import dstream.SerializableAssets.SerBiFunction;
import dstream.SerializableAssets.SerBinaryOperator;

/**
 * A conversion utility used to de-type and represent {@link SerBiFunction} 
 * as {@link SerBinaryOperator}. Typically used to promote code re-usability when 
 * implementing support for reducing and aggregation, since in a type-less world
 * the two are the same.<br>
 * See {@link ValuesAggregatingFunction} and {@link ValuesReducingFunction} for more details 
 * on its applicability.
 */
@SuppressWarnings("rawtypes")
public class BiFunctionToBinaryOperatorAdapter implements SerBinaryOperator<Object> {
	private static final long serialVersionUID = -2240524865400623818L;
	
	private final SerBiFunction bo;
	
	/**
	 * Constructs this function with the given {@link SerBiFunction} as a delegate.
	 */
	public BiFunctionToBinaryOperatorAdapter(SerBiFunction bo){
		this.bo = bo;
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object apply(Object t, Object u) {
		return this.bo.apply(t, u);
	}
}

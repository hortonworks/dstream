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
package io.dstream.utils;

import java.util.Iterator;

/**
 * Implementation of the {@link Iterator} over a single value.
 * Primarily used by aggregate operations to ensure type consistency of the
 * values in Key/Value pairs. <br>
 * When two or more values are aggregated they form
 * a natural collection which provides an {@link Iterator}.
 * If a particular value didn't get a chance to be combined with another value,
 * collection is not formed and value (in Key/Value pair) is represented <i>as is</i>
 * resulting in inconsistent value types.
 *
 * @param <T> the type of elements returned by this iterator
 */
public class SingleValueIterator<T> implements Iterator<T>{
	private final T v;
	boolean hasNext = true;

	/**
	 * Constructs a new {@link SingleValueIterator} over a value 'v'.
	 */
	public SingleValueIterator(T v) {
		this.v = v;
	}

	/**
	 *
	 */
	@Override
	public boolean hasNext() {
		return this.hasNext;
	}

	/**
	 *
	 */
	@Override
	public T next() {
		this.hasNext = false;
		return v;
	}
}

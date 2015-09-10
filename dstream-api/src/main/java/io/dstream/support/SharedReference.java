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
package io.dstream.support;

import java.io.Serializable;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Strategy to hold references to read-only values that may 
 * be shared across multiple JVM instances.<br>
 *
 * @param <T> type of element held by this reference. Must be {@link Serializable}.
 */
public abstract class SharedReference<T extends Serializable> implements Serializable {
	private static final long serialVersionUID = -4254764779886246749L;

	/**
	 * Factory method which creates an instance of {@link SharedReference}.
	 * It does so by using standard Java service loader mechanism (see {@link ServiceLoader}).<br>
	 * There is no basic implementation of this strategy, so each provider must provide its
	 * own implementation and specify it in <i>META-INF/services/dstream.support.SharedReference</i> 
	 * file.
	 * 
	 * @param object instance of {@link Serializable} object to hold as reference
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T extends Serializable> SharedReference<T> of(T object){
		Iterator<SharedReference> sl = ServiceLoader
	            .load(SharedReference.class, ClassLoader.getSystemClassLoader()).iterator();
		if (sl.hasNext()){
			SharedReference<T> sr = sl.next();
			sr.set(object);
			return sr;
		}
		throw new IllegalStateException("Failed to find '" + SharedReference.class.getName() + "' provider.");
	}

	/**
	 * Returns the value of referenced object
	 */
	public abstract T get();
	
	/**
	 * Sets the initial value. Must be implemented by sub classes.
	 */
	protected abstract void set(T value);
}

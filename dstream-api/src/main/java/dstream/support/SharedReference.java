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

import java.io.Serializable;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Strategy to hold references to objects that may be shared across multiple JVM instances.<br>
 * Keep in mind that depending on the implementation of this {@link SharedReference} any 
 * modifications made to a referenced object may not be visible, hence the referenced object is 
 * considered <i>immutable</i>.
 *
 * @param <T> type of element held by this reference. Must be {@link Serializable}.
 */
public interface SharedReference<T extends Serializable> extends Serializable {
	
	/**
	 * Factory method which creates an instance of {@link SharedReference}.
	 * It does so by using standard Java service loader mechanism (see {@link ServiceLoader}).<br>
	 * There is no basic implementation of this strategy, so each provider must provide its
	 * own implementation and specify it in <i>META-INF/services/org.apache.dstream.SharedReference</i> 
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
			if (sr instanceof MutableSharedReference){
				((MutableSharedReference<T>)sr).set(object);
				return sr;
			}	
			throw new IllegalStateException("Found '" + sr + "' provider which is not the instance "
					+ "MutableSharedReference, therefore initial value can not be set.");	
		}
		throw new IllegalStateException("Failed to find '" + SharedReference.class.getName() + "' provider.");
	}

	/**
	 * Returns the value of referenced object
	 * @return
	 */
	T get();
	
	/**
	 * Base extension of {@link SharedReference} which provides single mutator method 
	 * to set initial value of the {@link SharedReference}. Implementations must sub-class
	 * it instead of implementing {@link SharedReference}.
	 * 
	 * @param <T> type of element held by this reference. Must be {@link Serializable}.
	 */
	public abstract class MutableSharedReference<T extends Serializable> implements SharedReference<T>{	
		private static final long serialVersionUID = 1681211574566513484L;

		protected abstract void set(T object);
	}
}

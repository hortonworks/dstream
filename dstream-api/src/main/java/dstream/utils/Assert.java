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
package dstream.utils;

import java.util.Collection;

/**
 * Utility which provides common assertion operations.
 */
public class Assert {

	/**
	 * 
	 * @param number
	 */
	public static void numberGreaterThenZero(Number number) {
		if (number == null || number.intValue() == 0){
			throw new IllegalArgumentException("'number' must not be null and > 0");
		}
	}
	
	/**
	 * 
	 * @param collection
	 */
	public static void notEmpty(Collection<?> collection) {
		notEmpty(collection, "'collection' must not be null or empty");
	}
	
	/**
	 * 
	 * @param collection
	 * @param message
	 */
	public static void notEmpty(Collection<?> collection, String message) {
		if (collection == null || collection.size() == 0){
			throw new IllegalArgumentException(message);
		}
	}
	
	/**
	 * 
	 * @param array
	 */
	public static <T> void notEmpty(T[] array) {
		notEmpty(array, "'array' must not be null or empty");
	}
	
	/**
	 * 
	 * @param array
	 * @param message
	 */
	public static <T> void notEmpty(T[] array, String message) {
		if (array == null || array.length == 0){
			throw new IllegalArgumentException(message);
		}
	}
	
	/**
	 * 
	 * @param string
	 * @param message
	 */
	public static void notEmpty(String string, String message) {
		if (string == null || string.trim().length() == 0){
			throw new IllegalArgumentException(message);
		}
	}
	
	/**
	 * 
	 * @param _true
	 * @return
	 */
	public static boolean isTrue(boolean _true){
		return isTrue(_true, "Result of boolean expression is not true");
	}
	
	/**
	 * 
	 * @param _true
	 * @param message
	 * @return
	 */
	public static boolean isTrue(boolean _true, String message){
		if (!_true){
			throw new IllegalStateException(message);
		}
		return true;
	}
	
	/**
	 * 
	 * @param _false
	 * @return
	 */
	public static boolean isFalse(boolean _false){
		return isFalse(_false, "Result of boolean expression is not false");
	}
	
	/**
	 * 
	 * @param _false
	 * @param message
	 * @return
	 */
	public static boolean isFalse(boolean _false, String message){
		if (_false){
			throw new IllegalStateException(message);
		}
		return true;
	}
	
	/**
	 * 
	 * @param string
	 */
	public static void notEmpty(String string) {
		notEmpty(string, "'string' must not be null or empty");
	}
	
	/**
	 * 
	 * @param object
	 */
	public static <T> void notNull(T object) {
		notNull(object, "'object' must not be null");
	}
	
	/**
	 * 
	 * @param object
	 * @param message
	 */
	public static <T> void notNull(T object, String message) {
		if (object == null){
			throw new IllegalArgumentException(message);
		}
	}
}

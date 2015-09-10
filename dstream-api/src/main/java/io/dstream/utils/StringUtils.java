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

import java.util.Comparator;

/**
 * Utility class which contains common {@link String} operations
 */
public class StringUtils {

	/**
	 * Will compare the length of two strings, maintaining semantics of the
	 * traditional {@link Comparator} by returning <i>int</i>.
	 * @param a first {@link String}
	 * @param b second {@link String}
	 * @return integer value of -1, 0 or 1 following this logic:<br>
	 * <pre>
	 * a.length() &gt; b.length() ? 1 : a.length() &lt; b.length() ? -1 : 0;
	 * </pre>
	 */
	public static int compareLength(String a, String b){
		return a.length() > b.length() ? 1 : a.length() < b.length() ? -1 : 0;
	}
}

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


/**
 * Helper class which returns the partition id bound to the current thread.
 * Typically used to determine the partition id of currently processed 
 * element (within the function).
 *
 */
public class PartitionIdHelper {
	
	private final static ThreadLocal<Integer> partitionIdHolder = ThreadLocal.withInitial(() -> 0);
	
	/**
	 * Returns current partition id.
	 */
	public static int getPartitionId() {
		return partitionIdHolder.get();
	}
}

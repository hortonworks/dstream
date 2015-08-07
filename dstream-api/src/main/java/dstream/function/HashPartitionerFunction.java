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

/**
 * Implementation of the {@link PartitionerFunction} for hash based partitioning.
 *
 * @param <T> the type of the element that will be sent to a target partitioner
 * to determine partition id.
 */
public class HashPartitionerFunction<T> extends PartitionerFunction<T> {
	private static final long serialVersionUID = -3799649258371438298L;
	
	/**
	 * Constructs this function.
	 * 
	 * @param partitionSize the size of partitions
	 */
	public HashPartitionerFunction(int partitionSize){
		super(partitionSize);
	}

	/**
	 * 
	 */
	@Override
	public Integer apply(T input) {
		Object hashValue = input;
		if (this.getClassifier() != null){
			hashValue = this.getClassifier().apply(input);
		}
		int ret = (hashValue.hashCode() & Integer.MAX_VALUE) % this.getPartitionSize();
		return ret;
	}
}

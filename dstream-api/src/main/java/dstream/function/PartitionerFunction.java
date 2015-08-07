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

import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.utils.Assert;

/**
 * Base implementation of the  partitioner to be used by the target system.
 * 
 * @param <T> the type of the element that will be sent to a target partitioner
 * to determine partition id.
 */
public abstract class PartitionerFunction<T> implements SerFunction<T, Integer> {
	private static final long serialVersionUID = -250807397502312547L;
	
	private final int partitionSize;
	
	private SerFunction<? super T, ?> classifier;

	/**
	 * Constructs this function.
	 * 
	 * @param partitionSize the size of partitions
	 */
	public PartitionerFunction(int partitionSize) {
		Assert.isTrue(partitionSize > 0, "'partitionSize' must be > 0");
		this.partitionSize = partitionSize;
	}

	/**
	 * @return the size of partitions
	 */
	public int getPartitionSize(){
		return this.partitionSize;
	}
	
	/**
	 * Allows to set the classifier {@link SerFunction} function to extract value 
	 * used to determine partition id.
	 * 
	 * @param classifier function to extract value used by a target partitioner.
	 */
	public void setClassifier(SerFunction<? super T, ?> classifier) {
		this.classifier = classifier;
	}
	
	/**
	 * Returns <i>classifier</i> function used by the instance of this partitioner.
	 * Could be <i>null</i> if not set.
	 * 
	 * @return function to extract value used by a target partitioner.
	 */
	public SerFunction<? super T, ?> getClassifier() {
		return this.classifier;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return this.getClass().getSimpleName() + ":" + this.partitionSize;
	}
}

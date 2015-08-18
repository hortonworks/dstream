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

import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.utils.Assert;

/**
 * Base implementation of the  partitioner to be used by the target system.
 * 
 * @param <T> the type of the element that will be sent to a target partitioner
 * to determine partition id.
 */
public abstract class Classifier implements Serializable {
	private static final long serialVersionUID = -250807397502312547L;
	
	private final int groupSize;
	
	private SerFunction<Object, ?> classificationValueMapper;

	/**
	 * Constructs this function.
	 * 
	 * @param partitionSize the size of partitions
	 */
	public Classifier(int groupSize) {
		Assert.isTrue(groupSize > 0, "'groupSize' must be > 0");
		this.groupSize = groupSize;
	}
	
	
	public Integer getClassificationId(Object input) {
		int partId = this.doGetClassificationId(input);
		return partId;
	}
	
	protected abstract int doGetClassificationId(Object input);

	/**
	 * @return the size of partitions
	 */
	public int getSize(){
		return this.groupSize;
	}
	
	/**
	 * Allows to set the classifier {@link SerFunction} function to extract value 
	 * used to determine partition id.
	 * 
	 * @param classifier function to extract value used by a target partitioner.
	 */
	public void setClassificationValueMapper(SerFunction<Object, ?> classificationValueMapper) {
		this.classificationValueMapper = classificationValueMapper;
	}
	
	/**
	 * Returns <i>classifier</i> function used by the instance of this partitioner.
	 * Could be <i>null</i> if not set.
	 * 
	 * @return function to extract value used by a target partitioner.
	 */
	public SerFunction<Object, ?> getClassificationValueMapper() {
		return this.classificationValueMapper;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return this.getClass().getSimpleName() + ":" + this.groupSize;
	}
}

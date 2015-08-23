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

import dstream.SerializableStreamAssets.SerFunction;
import dstream.utils.Assert;

/**
 * Base implementation of the classification functionality.<br>
 * Classification could be looked at as the process of distributed grouping and 
 * in the "distributable" reality, often coincides with data <i>partitioning</i>. 
 * Since {@link Classifier} is compliant with the general semantics of partitioners
 * by returning an {@link Integer} from {@link #getClassificationId(Object)} method, 
 * this id could be treated by a target partitioner as partition id.<br>. 
 */
public abstract class Classifier implements Serializable {
	private static final long serialVersionUID = -250807397502312547L;
	
	private final int classificationSize;
	
	private SerFunction<Object, ?> classificationValueMapper;

	/**
	 * Constructs this instance with <i>classificationSize</i>
	 */
	public Classifier(int classificationSize) {
		Assert.isTrue(classificationSize > 0, "'classificationSize' must be > 0");
		this.classificationSize = classificationSize;
	}
	
	/**
	 * Returns classification if computed on the instance of <i>input</i>.
	 */
	public Integer getClassificationId(Object input) {
		int partId = this.doGetClassificationId(input);
		return partId;
	}

	/**
	 * Returns the total amount of classifications
	 */
	public int getSize(){
		return this.classificationSize;
	}
	
	/**
	 * Allows to set/reset an instance of the {@link SerFunction} which maps the value to be 
	 * used to compute classification.
	 * <pre>
	 * dstream.classify(str -> str.substring(0, 5))
	 * </pre>
	 * Assuming that the value passed to the classify operation is "Hello Washington", the 
	 * classification will be performed using only "Hello" string based on the 
	 * given function (str -> str.substring(0, 5)).
	 */
	public void setClassificationValueMapper(SerFunction<Object, ?> classificationValueMapper) {
		this.classificationValueMapper = classificationValueMapper;
	}
	
	/**
	 * Returns and instance of the {@link SerFunction} which maps the value to be 
	 * used to compute classification.
	 */
	public SerFunction<Object, ?> getClassificationValueMapper() {
		return this.classificationValueMapper;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return this.getClass().getSimpleName() + ":" + this.classificationSize;
	}
	
	/**
	 * An abstract delegate method to be implemented by sub-classes
	 * which implements the actual classification logic.
	 */
	protected abstract int doGetClassificationId(Object input);
}

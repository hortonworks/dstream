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

import java.util.Map.Entry;

/**
 * Implementation of the {@link Classifier} for hash based classifications 
 *
 */
public class HashClassifier extends Classifier {
	private static final long serialVersionUID = -3799649258371438298L;
	
	/**
	 * Constructs this instance with <i>classificationSize</i>
	 */
	public HashClassifier(int groupSize){
		super(groupSize);
	}

	/**
	 * Returns <i>int</i> representing the result of the hash based classification
	 * on value <i>input</i>.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int doGetClassificationId(Object input) {
		Object hashValue = input instanceof Entry ? ((Entry)input).getKey() : input;
		
		int groupId = (hashValue.hashCode() & Integer.MAX_VALUE) % this.getSize();
		return groupId;
	}
}

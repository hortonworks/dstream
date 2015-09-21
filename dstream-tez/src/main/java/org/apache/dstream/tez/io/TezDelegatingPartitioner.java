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
package org.apache.dstream.tez.io;

import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import io.dstream.support.Classifier;

/**
 *
 */
public class TezDelegatingPartitioner extends HashPartitioner {

	private static Classifier delegatingClassifier;

	public static void setDelegator(Classifier classifier){
		delegatingClassifier = classifier;
	}

	/**
	 *
	 */
	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
		return this.doGetPartition((KeyWritable)key, (ValueWritable<?>)value, numPartitions);
	}

	/**
	 *
	 */
	private int doGetPartition(KeyWritable key, ValueWritable<?> value, int numPartitions) {
		int partitionId;
		Object valueToUse = key;
		if (key.getValue() == null){
			valueToUse = value.getValue();
		}
		else {
			valueToUse = key.getValue();
		}
		if (delegatingClassifier != null){
			partitionId = delegatingClassifier.getClassificationId(valueToUse);
		}
		else {
			partitionId = super.getPartition(valueToUse, null, numPartitions);
		}
		return partitionId;
	}
}

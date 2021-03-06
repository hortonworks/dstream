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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.dstream.support.HashClassifier;

public class HashClassifierTests {

	@Test(expected=IllegalStateException.class)
	public void failWithLessThenOnePartitionSize(){
		new HashClassifier(0);
	}
	
	@Test
	public void validateHashGrouper(){
		HashClassifier hp = new HashClassifier(4);
		assertEquals(4, hp.getSize());
		assertEquals((Integer)1, hp.getClassificationId("a"));
		assertEquals((Integer)2, hp.getClassificationId("b"));
		assertEquals((Integer)3, hp.getClassificationId("c"));
		assertEquals((Integer)0, hp.getClassificationId("d"));
	}
}

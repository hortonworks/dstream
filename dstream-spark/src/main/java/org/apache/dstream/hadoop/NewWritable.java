/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dstream.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 */
public interface NewWritable<T> extends Writable {
	/**
	 * 
	 * @param value
	 */
	void setValue(T value);
	/**
	 * 
	 * @return
	 */
	T getValue();
	
	/**
	 * 
	 */
	static class NewIntWritable extends IntWritable implements NewWritable<Integer> {
		@Override
		public void setValue(Integer value) {
			super.set(value);
		}

		@Override
		public Integer getValue() {
			return super.get();
		}
	}
	/**
	 * 
	 */
	static class NewLongWritable extends LongWritable implements NewWritable<Long> {
		@Override
		public void setValue(Long value) {
			super.set(value);
		}

		@Override
		public Long getValue() {
			return super.get();
		}
	}
	/**
	 * 
	 */
	static class NewTextWritable extends Text implements NewWritable<Object> {
		@Override
		public void setValue(Object value) {
			super.set(value.toString());
		}

		@Override
		public String getValue() {
			return super.toString();
		}
	}
}

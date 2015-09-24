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
package io.dstream.tez.utils;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesReader;

import io.dstream.tez.io.KeyWritable;
import io.dstream.tez.io.ValueWritable;
import io.dstream.utils.KVUtils;
/**
 *
 */
public class StreamUtils {

	/**
	 *
	 * @param kvReader
	 * @return
	 *
	 * @param <K> key type
	 * @param <V> value type
	 */
	public static <K,V> Stream<Entry<K,V>> toStream(KeyValueReader kvReader) {
		KeyValueReaderIterator<K,V> kvIterator = new KeyValueReaderIterator<K, V>(kvReader);
		Stream<Entry<K,V>> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(kvIterator, Spliterator.ORDERED), false);
		return targetStream;
	}

	/**
	 *
	 * @param kvsReader
	 * @return
	 *
	 * @param <K> key type
	 * @param <V> value type
	 */
	public static <K,V> Stream<Entry<K,Iterator<V>>> toStream(KeyValuesReader kvsReader) {
		KeyValuesReaderIterator<K,V> kvsIterator = new KeyValuesReaderIterator<K, V>(kvsReader);
		Stream<Entry<K,Iterator<V>>> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(kvsIterator, Spliterator.ORDERED), false);
		return targetStream;
	}

	/**
	 *
	 */
	private static class KeyValuesReaderIterator<K,V> implements Iterator<Entry<K,Iterator<V>>> {
		private final KeyValuesReader kvsReader;

		private Iterator<V> currentValues;

		public KeyValuesReaderIterator(KeyValuesReader kvsReader) {
			this.kvsReader = kvsReader;
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean hasNext() {
			try {
				boolean hasNext = false;
				if (this.currentValues == null){
					if (this.kvsReader.next()){
						this.currentValues = (Iterator<V>) this.kvsReader.getCurrentValues().iterator();
						hasNext = this.currentValues.hasNext();
					}
				} else {
					//					if (this.currentValues.hasNext()){
					//						hasNext = true;
					//					} else {
					if (this.kvsReader.next()){
						this.currentValues = (Iterator<V>) this.kvsReader.getCurrentValues().iterator();
						hasNext = this.currentValues.hasNext();
					}
					//					}
				}
				return hasNext;
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Entry<K, Iterator<V>> next() {
			try {
				K key = (K) ((KeyWritable)this.kvsReader.getCurrentKey()).getValue();
				Iterator<V> values = new Iterator<V>() {
					@Override
					public boolean hasNext() {
						return currentValues.hasNext();
					}

					@Override
					public V next() {
						return ((ValueWritable<V>)currentValues.next()).getValue();
					}
				};
				Entry<K, Iterator<V>> entry =  KVUtils.kv(key, values);
				return entry;
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}

	/**
	 *
	 */
	private static class KeyValueReaderIterator<K,V> implements Iterator<Entry<K,V>> {
		private final KeyValueReader kvReader;

		public KeyValueReaderIterator(KeyValueReader kvReader) {
			this.kvReader = kvReader;
		}
		@Override
		public boolean hasNext() {
			try {
				return this.kvReader.next();
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Entry<K, V> next() {
			try {
				Entry<K, V> entry = (Entry<K, V>) KVUtils.kv(this.kvReader.getCurrentKey(), this.kvReader.getCurrentValue());
				return entry;
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}
}

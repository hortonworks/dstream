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
package io.dstream.hadoop;

import io.dstream.utils.KVUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
/**
 *
 * @param <T>
 */
public class SequenceFileOutputStreamsBuilder<T> {

	private final FileSystem fs;
	
	private final String outputPath;
	
	private final Configuration configuration;
	
	public SequenceFileOutputStreamsBuilder(FileSystem fs, String outputPath, Configuration configuration){
		this.fs = fs;
		this.outputPath = outputPath;
		this.configuration = configuration;
	}
	
	@SuppressWarnings("unchecked")
	public Stream<T>[] build() {
		List<Stream<T>> outputStreams = new ArrayList<Stream<T>>();
		FileStatus[] fileStatuses;
		try {
			fileStatuses = fs.listStatus(new org.apache.hadoop.fs.Path(this.outputPath));
		} catch (Exception e) {
			throw new IllegalStateException("Failed to obtain File Statuses", e);
		}

		for (FileStatus fileStatus : fileStatuses) {
			if (!fileStatus.getPath().toString().endsWith("_SUCCESS")){
				Iterator<T> resultIterator = new Iterator<T>() {
					private final KeyWritable key = new KeyWritable();
					private final ValueWritable<?> value = new ValueWritable<>();
					SequenceFile.Reader reader = null;
					@Override
					public boolean hasNext() {
						boolean hasNext = false;
						if (reader == null){
							try {
								this.reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(fileStatus.getPath()));
							} catch (Exception e) {
								throw new IllegalStateException("Failed to create Sequence File Reader for path: " + fileStatus.getPath(), e);
							}
						}
						try {
							hasNext = reader.next(key, value);
						} catch (Exception e) {
							try {
								this.reader.close();
							} catch (Exception ex) {/*ignore*/}
							throw new IllegalStateException("Failed reade Sequence File: " + fileStatus.getPath(), e);
						} 
						if (!hasNext){
							try {
								this.reader.close();
							} catch (Exception ex) {/*ignore*/}
						}
						return hasNext;
					}

					@Override
					public T next() {
						Object key = this.key.getValue();
						Object value = this.value.getValue();
						if (key == null){
							return (T) value;
						}
						else {
							return (T) KVUtils.kv(key, value);
						}
					}
				};
				Stream<T> targetStream = (Stream<T>) StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false);
				outputStreams.add(targetStream);
			}
		}
		return outputStreams.toArray(new Stream[outputStreams.size()]);
	}
}

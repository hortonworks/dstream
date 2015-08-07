package org.apache.dstream.tez.utils;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.tez.OutputStreamsBuilder;
import org.apache.dstream.tez.io.KeyWritable;
import org.apache.dstream.tez.io.ValueWritable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.tez.dag.api.TezConfiguration;

import dstream.utils.KVUtils;

public class SequenceFileOutputStreamsBuilder<T> implements OutputStreamsBuilder<T> {

	private final FileSystem fs;
	
	private final String outputPath;
	
	private final TezConfiguration tezConfiguration;
	
	public SequenceFileOutputStreamsBuilder(FileSystem fs, String outputPath, TezConfiguration tezConfiguration){
		this.fs = fs;
		this.outputPath = outputPath;
		this.tezConfiguration = tezConfiguration;
	}
	
	@SuppressWarnings("unchecked")
	@Override
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
								this.reader = new SequenceFile.Reader(tezConfiguration, SequenceFile.Reader.file(fileStatus.getPath()));
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

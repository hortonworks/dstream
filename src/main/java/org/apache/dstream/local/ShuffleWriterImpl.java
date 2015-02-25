package org.apache.dstream.local;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.assembly.ShuffleWriter;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <K>
 * @param <V>
 */
public class ShuffleWriterImpl<K,V> implements ShuffleWriter<K, V> {
	
	private final Logger logger = LoggerFactory.getLogger(ShuffleWriterImpl.class);
	
	private final SerializableFunction<Entry<K,V>, Integer> partitioner;
	
	private final Map<Integer, ConcurrentHashMap<K,V>> partitions;
	
	private final BinaryOperator<V> mergerFunction;

	public ShuffleWriterImpl(Map<Integer, ConcurrentHashMap<K, V>> partitions, SerializableFunction<Entry<K,V>, Integer> partitioner, BinaryOperator<V> mergerFunction){
		this.partitioner = partitioner;
		this.partitions = partitions;
		this.mergerFunction = mergerFunction;
	}
	
	@Override
	public void write(Entry<K, V> pair) {
		int partitionId = partitioner.apply(pair);
		ConcurrentHashMap<K, V> partition = this.partitions.get(partitionId);
		partition.merge(pair.getKey(), pair.getValue(), this.mergerFunction);
	}

	/**
	 * 
	 * @return
	 */
	public StreamableSource<Entry<K,V>> toStreamableSource(){
		StreamableSource<Entry<K,V>> source = new StreamableSource<Map.Entry<K,V>>() {
			@Override
			public Stream<Entry<K, V>> toStream() {
				EntryIterator shuffleIterator = new EntryIterator();
				Stream<Entry<K,V>> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(shuffleIterator, Spliterator.ORDERED), false);
				return targetStream;
			}
		};
		return source;
	}
	
	/**
	 * 
	 */
	private class EntryIterator implements Iterator<Entry<K,V>> {
		private int currentPartitionId;
		private Iterator<Entry<K,V>> currentPartitionIterator;
		@Override
		public boolean hasNext() {
			if (currentPartitionId < partitions.size()){
				if (currentPartitionIterator == null || !currentPartitionIterator.hasNext()){
					if (logger.isInfoEnabled()){
						logger.info("Iterating over partition: " + currentPartitionId);
					}
					currentPartitionIterator = partitions.get(currentPartitionId++).entrySet().iterator();
				}
				return currentPartitionIterator.hasNext();
			}
			return false;
		}
		@Override
		public Entry<K,V> next() {
			return currentPartitionIterator.next();
		}
	}
}

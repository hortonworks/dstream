package org.apache.dstream.local;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;

import org.apache.dstream.Pipeline;
import org.apache.dstream.assembly.Writer;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <K>
 * @param <V>
 */
public class ShuffleWriterImpl<K,V> implements Writer<Entry<K,V>> {
	
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
	public Pipeline<Entry<K,V>> toStreamableSource(){
//		DistributedPipeline<Entry<K,V>> distributedPipeline = new DefaultDistributedPipeline<Entry<K,V>>(jobName)
//		ComputableSource<Entry<K,V>> source = new ComputableSource<Map.Entry<K,V>>() {
//			@Override
//			public Stream<Entry<K, V>> toStream() {
//				EntryIterator shuffleIterator = new EntryIterator();
//				Stream<Entry<K,V>> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(shuffleIterator, Spliterator.ORDERED), false);
//				return targetStream;
//			}
//
//			@Override
//			public SerializableFunction<Stream<?>, Stream<?>> getPreprocessFunction() {
//				// TODO Auto-generated method stub
//				return null;
//			}
//
//			@Override
//			public void setPreprocessFunction(
//					SerializableFunction<Stream<?>, Stream<?>> preProcessFunction) {
//				// TODO Auto-generated method stub
//				
//			}
//
//			@Override
//			public ComputableSource<Entry<K, V>> preProcessSource(
//					SerializableFunction<Path[], Path[]> sourcePreProcessFunction) {
//				// TODO Auto-generated method stub
//				return null;
//			}
//
//			
//		};
		return null;
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

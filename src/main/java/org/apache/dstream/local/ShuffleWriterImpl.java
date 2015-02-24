package org.apache.dstream.local;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;

import org.apache.dstream.assembly.ShuffleWriter;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 * @param <K>
 * @param <V>
 */
public class ShuffleWriterImpl<K,V> implements ShuffleWriter<K, V> {
	
	private final SerializableFunction<K, Integer> partitioner;
	
	private final Map<Integer, ConcurrentHashMap<K,V>> partitions;
	
	private final BinaryOperator<V> mergerFunction;

	public ShuffleWriterImpl(Map<Integer, ConcurrentHashMap<K, V>> partitions, SerializableFunction<K, Integer> partitioner, BinaryOperator<V> mergerFunction){
		this.partitioner = partitioner;
		this.partitions = partitions;
		this.mergerFunction = mergerFunction;
	}
	
	@Override
	public void write(K key, V value) {
		int partitionId = partitioner.apply(key);
		ConcurrentHashMap<K, V> partition = this.partitions.get(partitionId);
		partition.merge(key, value, this.mergerFunction);
	}

	public StreamableSource<Entry<K,V>> toStreamableSource(){
		return null;
	}
}

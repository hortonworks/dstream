package org.apache.dstream;

import java.util.Map.Entry;


public interface KeyValueProcessor<T,K,V> extends Processor<Void, T, Entry<K, V>> {

	@Override
	Void process(ItemReader<T> reader, ResultWriter<Entry<K, V>> writer);
}

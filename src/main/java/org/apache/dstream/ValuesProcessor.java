package org.apache.dstream;

public interface ValuesProcessor<T,V> extends Processor<Void, T, V>{

	@Override
	Void process(ItemReader<T> reader, ResultWriter<V> writer);
}

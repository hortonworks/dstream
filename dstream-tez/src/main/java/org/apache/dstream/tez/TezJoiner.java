package org.apache.dstream.tez;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.support.Joiner;
import org.apache.dstream.utils.KVUtils;

class TezJoiner extends Joiner {
	private static final long serialVersionUID = -2554454163443511159L;

	public TezJoiner(Predicate<?>... predicates) {
		super(predicates);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Object preProcessValue(Object v) {
		Stream<Entry<Object, Iterator<?>>> stream = (Stream<Entry<Object, Iterator<?>>>) v;
		
		return stream.flatMap(s -> {
			Iterator iter = new Iterator(){

				@Override
				public boolean hasNext() {
					return s.getValue().hasNext();
				}

				@Override
				public Object next() {
					if (s.getKey() == null){
						return  s.getValue().next();
					}
					else {
						return KVUtils.kv(s.getKey(), s.getValue().next());
					}
				}
				
			};
			return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
		});
	}
}

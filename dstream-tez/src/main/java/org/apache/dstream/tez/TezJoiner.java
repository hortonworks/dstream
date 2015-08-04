package org.apache.dstream.tez;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.function.StreamJoinerFunction;

class TezJoiner extends StreamJoinerFunction {
	private static final long serialVersionUID = -2554454163443511159L;
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Stream<?> preProcessStream(Stream<?> stream) {
		return KeyValuesNormalizer.normalize((Stream<Entry<Object, Iterator<Object>>>) stream);
//		Stream<Entry<Object, Iterator<?>>> entryStream = (Stream<Entry<Object, Iterator<?>>>) stream;
//		
//		return entryStream.flatMap(s -> {
//			Iterator iter = new Iterator(){
//
//				@Override
//				public boolean hasNext() {
//					return s.getValue().hasNext();
//				}
//
//				@Override
//				public Object next() {
//					if (s.getKey() == null){
//						return  s.getValue().next();
//					}
//					else {
//						return KVUtils.kv(s.getKey(), s.getValue().next());
//					}
//				}
//				
//			};
//			return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
//		});
	}
}

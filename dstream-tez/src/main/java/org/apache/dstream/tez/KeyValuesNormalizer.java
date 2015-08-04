package org.apache.dstream.tez;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.utils.KVUtils;

public interface KeyValuesNormalizer {
	
	/**
	 * 
	 * @param stream
	 * @return
	 */
	public static Stream<Object> normalize(Stream<Entry<Object, Iterator<Object>>> stream) {
	
		return stream.flatMap(s -> {
			Iterator<Object> iter = new Iterator<Object>(){

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

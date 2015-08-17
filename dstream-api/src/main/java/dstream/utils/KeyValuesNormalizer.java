package dstream.utils;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import dstream.utils.KVUtils;

public interface KeyValuesNormalizer {

	/**
	 * 
	 * @param stream
	 * @return
	 */
	public static Stream<?> normalizeStream(Stream<?> stream) {
	
		return stream.flatMap(s -> {
			Iterator<Object> iter = new Iterator<Object>(){
				boolean hasNext = true;
				@Override
				public boolean hasNext() {
					if (s instanceof Entry){
						Entry<Object, Iterator<Object>> entry = (Entry) s;
						return entry.getValue().hasNext();
					}
					return hasNext;
				}

				@Override
				public Object next() {
					if (s instanceof Entry){
						Entry<Object, Iterator<Object>> entry = (Entry) s;
						if (entry.getKey() == null){
							return  entry.getValue().next();
						}
						else {
							return KVUtils.kv(entry.getKey(), entry.getValue().next());
						}
					}
					else {
						hasNext = false;
						return s;
					}
				}
				
			};
			return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
		});
	}

}

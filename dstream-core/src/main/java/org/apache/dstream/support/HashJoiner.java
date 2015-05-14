package org.apache.dstream.support;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.utils.Pair;

public class HashJoiner implements Serializable {
	private static final long serialVersionUID = 5957573722149836021L;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <B,P,R> Stream<R> join(Stream<B> hashStream, Stream<P> probeStream) {
		
		Map<Object, Object> joined = new HashMap<Object, Object>();
		
		hashStream.forEach((Consumer<? super B>) (Entry entry) -> {
			Stream<?> valuesStream = (Stream<?>) StreamSupport.stream(Spliterators.spliteratorUnknownSize((Iterator<?>) entry.getValue(), Spliterator.ORDERED), false);
			List<?> values = valuesStream.collect(Collectors.toList());
			joined.put(entry.getKey(), values);
	    });
		
		probeStream.forEach((Consumer<? super P>) (Entry entry) -> {
			Stream<?> valuesStream = (Stream<?>) StreamSupport.stream(Spliterators.spliteratorUnknownSize((Iterator<?>) entry.getValue(), Spliterator.ORDERED), false);
			List<?> values = valuesStream.collect(Collectors.toList());
			joined.merge(entry.getKey(), values, (a, b) -> Pair.of(a, b));
		});
		
		return (Stream<R>) joined.entrySet().stream();
	}
}

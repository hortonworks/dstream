package org.apache.dstream.io;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

public class CollectionStreamableSource<T> implements StreamableSource<T> {
	private final Collection<T> collection;
	
	private CollectionStreamableSource(Collection<T> collection){
		Objects.requireNonNull(collection, "'collection' must not be null");
		
		this.collection = collection;
	}
	
	public static <T> CollectionStreamableSource<T> create(Collection<T> collection){
		return new CollectionStreamableSource<T>(collection);
	}

	@Override
	public Stream<T> toStream() {
		/*
		 * for now. Later we need to address collection split similar to the 'parallelize'
		 */
		return collection.stream();
	}

}

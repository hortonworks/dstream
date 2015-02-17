package org.apache.dstream.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.utils.Assert;

/**
 * 
 * @param <T>
 */
public class ListStreamableSource<T> implements StreamableSource<T> {
	private final List<T> list;
	
	private final int subsetSize;
	
	private ListStreamableSource(List<T> list, int splits){
		Objects.requireNonNull(list, "'list' must not be null");
		Assert.numberGreaterThenZero(splits);
		
		if (list.size() < splits){
			throw new IllegalArgumentException("Collection is too small (" + list.size() 
					+ ") to be parallelized as " + splits + "  splits");
		}
		
		
		this.subsetSize = list.size()/splits;
		
		System.out.println(subsetSize);
		
		this.list = list;
	}
	
	/**
	 * Factory method to create {@link StreamableSource} for a provided {@link Collection}
	 * 
	 * @param collection
	 * @return
	 */
	public static <T> ListStreamableSource<T> create(List<T> list){
		return new ListStreamableSource<T>(list, 1);
	}
	
	/**
	 * Factory method to create {@link StreamableSource} for a provided {@link Collection}.
	 * This method also allows you to specify the amount of splits. The provided collection 
	 * will be divided (split) based on the amount of splits provided resulting in subsets of 
	 * collection to be processed in parallel 
	 * 
	 * @param collection
	 * @param partitions
	 * @return
	 */
	public static <T> ListStreamableSource<T> create(List<T> list, int splits){
		return new ListStreamableSource<T>(list, splits);
	}
	
	/**
	 * Will return {@link Stream} on the entire {@link Collection} regardless of how many splits was requested.
	 * To request a {@link Stream} for a particular split, see {@link #toStream(int)} method which
	 * allows you to provide partition ID.
	 */
	@Override
	public Stream<T> toStream() {
		return list.stream();
	}
	
	/**
	 *  Will return {@link Stream} for a collection subset identified by the partition ID.
	 *  
	 * @param partitionId
	 * @return
	 */
	public Stream<T> toStream(int partitionId) {
		int startIndex = partitionId * this.subsetSize;
		int endIndex = partitionId + this.subsetSize;
		
		Iterator<T> iterator = new Iterator<T>() {
			int currentIndex = startIndex;
			@Override
			public boolean hasNext() {
				return currentIndex < endIndex 	&& currentIndex < list.size();
			}

			@Override
			public T next() {
				return list.get(currentIndex++);
			}
		};
		
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iterator,
                Spliterator.ORDERED | Spliterator.IMMUTABLE), false);
	}

}

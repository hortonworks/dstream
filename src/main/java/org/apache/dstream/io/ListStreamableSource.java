package org.apache.dstream.io;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.dstream.utils.Assert;

/**
 * 
 * @param <T>
 */
public class ListStreamableSource<T> implements StreamableSource<T> {
	private final List<T> list;
	
	private final int splits;

	private ListStreamableSource(List<T> list, int splits) {
		Objects.requireNonNull(list, "'list' must not be null");
		Assert.numberGreaterThenZero(splits);

		if (list.size() < splits) {
			throw new IllegalArgumentException("Collection is too small ("
					+ list.size() + ") to be parallelized as " + splits
					+ "  splits");
		}
		this.splits = splits;
		this.list = list;
	}

	/**
	 * Factory method to create {@link StreamableSource} for a provided
	 * {@link Collection}
	 * 
	 * @param collection
	 * @return
	 */
	public static <T> ListStreamableSource<T> create(List<T> list) {
		return new ListStreamableSource<T>(list, 1);
	}

	/**
	 * Factory method to create {@link StreamableSource} for a provided
	 * {@link Collection}. This method also allows you to specify (at most) the amount of
	 * splits the provided list have to be divided to allow parallel processing. 
	 * 
	 * @param collection
	 * @param partitions
	 * @return
	 */
	public static <T> ListStreamableSource<T> create(List<T> list, int splits) {
		return new ListStreamableSource<T>(list, splits);
	}

	/**
	 * Will return {@link Stream} on the entire {@link Collection} regardless of
	 * how many splits was requested. To request a {@link Stream} for a
	 * particular split, see {@link #toStream(int)} method which allows you to
	 * provide partition ID.
	 */
	@Override
	public Stream<T> toStream() {
		return list.stream();
	}

	/**
	 * Will return {@link Stream} for a collection subset identified by the
	 * partition ID.
	 * 
	 * @param partitionId
	 * @return
	 */
	public Stream<T> toStream(int partitionId) {
		int actualSplits = this.list.size()/this.splits+1;
		int counter = 0;
		List<T> subList = null;
	
	    for (int start = 0; start < this.list.size() || counter < partitionId; start += actualSplits) {
	        if (counter == partitionId){
	        	int end = Math.min(start + actualSplits, list.size());
	        	subList = list.subList(start, end);
	        } 
	        counter++;
	    }
	    if (subList == null){
	    	throw new IllegalStateException("Split can not be determined for partition: " + partitionId);
	    }
		return subList.stream();
	}
}

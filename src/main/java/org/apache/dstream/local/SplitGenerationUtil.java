package org.apache.dstream.local;

import org.apache.dstream.io.StreamableSource;

class SplitGenerationUtil {

	/**
	 * This methods will generate splits from {@link StreamableSource} to mainly emulate the behavior of
	 * the underlying distributed system allowing {@link StreamableSource} to be processed parallelized as if
	 * it was parallelized in such system.
	 * 
	 * @param source
	 */
	public static <T> Split<T>[] generateSplits(StreamableSource<T> source){
		return null;
	}
}

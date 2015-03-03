package org.apache.dstream.local;

import org.apache.dstream.DistributedPipeline;
import org.apache.dstream.io.TextSource;

class SplitGenerationUtil {

	/**
	 * This methods will generate splits from {@link ComputableSource} to mainly emulate the behavior of
	 * the underlying distributed system allowing {@link ComputableSource} to be processed parallelized as if
	 * it was parallelized in such system.
	 * 
	 * @param source
	 */
	public static <T> Split<T>[] generateSplits(DistributedPipeline<T> source){
		Split<T>[] splits = null;
		if (source instanceof TextSource){
			splits = PourManTextFileSplitter.generateSplits((TextSource) source);
		} else {
			throw new UnsupportedOperationException("Source " + source + " is not currently supported");
		}
		return splits;
	}
}

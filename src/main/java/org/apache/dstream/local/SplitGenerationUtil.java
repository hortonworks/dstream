package org.apache.dstream.local;

import org.apache.dstream.io.StreamSource;
import org.apache.dstream.io.TextSource;

class SplitGenerationUtil {

	/**
	 * This methods will generate splits from {@link StreamSource} to mainly emulate the behavior of
	 * the underlying distributed system allowing {@link StreamSource} to be processed parallelized as if
	 * it was parallelized in such system.
	 * 
	 * @param source
	 */
	public static <T> Split<T>[] generateSplits(StreamSource<T> source){
		Split<T>[] splits = null;
		if (source instanceof TextSource){
			splits = PourManTextFileSplitter.generateSplits((TextSource) source);
		} else {
			throw new UnsupportedOperationException("Source " + source + " is not currently supported");
		}
		return splits;
	}
}

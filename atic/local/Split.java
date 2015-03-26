package org.apache.dstream.local;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Split<T> {

	private final File file;
	
	private final int start;
	
	private final int length;
	
	public Split(File file, int start, int length) {
		this.file = file;
		this.start = start;
		this.length = length;
	}
	
	@SuppressWarnings("unchecked")
	public Stream<T> toStream() {
		try {
			FileInputStream fis = new FileInputStream(this.file);
			fis.skip(this.start);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
			Iterator<String> splitSourceIterator = new LineReadingIterator(reader);

			Stream<String> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(splitSourceIterator, Spliterator.ORDERED), false);
			return (Stream<T>) targetStream;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * 
	 */
	private class LineReadingIterator implements Iterator<String> {
		private int bytesReadCount = 0;
		private final BufferedReader reader;
		public LineReadingIterator(BufferedReader reader){
			this.reader = reader;
		}
		@Override
		public boolean hasNext() {
			return this.bytesReadCount < Split.this.length-1;
		}
		@Override
		public String next() {
			try {
				String line = this.reader.readLine();
				int lineLength = line.length();
				this.bytesReadCount += lineLength;
				return line;
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}
}

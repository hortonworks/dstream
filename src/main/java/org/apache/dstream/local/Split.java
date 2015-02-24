package org.apache.dstream.local;

import java.io.File;
import java.util.stream.Stream;

public class Split<T> {

	private final File file;
	
	private final int start;
	
	private final int length;
	
	public Split(File file, int start, int length) {
		this.file = file;
		this.start = start;
		this.length = length;
	}
	
	public Stream<T> toStream(){
		return null;
	}
}

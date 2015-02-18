package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.stream.Stream;

public class TextSource extends KeyValueFsStreamableSource<Long, String> {
	

	protected TextSource(Path path) {
		super(Long.class, String.class, path);
		
	}
	
	public static TextSource create(Path path) {
		return new TextSource(path);
	}

	@Override
	public Stream<String> toStream() {
		// TODO Auto-generated method stub
		return null;
	}
}

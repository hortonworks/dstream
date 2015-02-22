package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TextSource extends KeyValueFsStreamableSource<Long, String> {
	

	private TextSource(Path path) {
		super(Long.class, String.class, new Supplier<Path[]>() {
			@Override
			public Path[] get() {
				return new Path[]{path};
			}
		});	
	}
	
	private TextSource(Supplier<Path[]> sourceSupplier) {
		super(Long.class, String.class, sourceSupplier);	
	}
	
	public static TextSource create(Path path) {
		return new TextSource(path);
	}
	
	/**
	 * Factory method which allows to create TextSource
	 */
	public static TextSource create(Supplier<Path[]> sourceSupplier) {
		return null;
	}

	@Override
	public Stream<String> toStream() {
		// TODO Auto-generated method stub
		return null;
	}
}

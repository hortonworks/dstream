package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.dstream.utils.Assert;

public class TextSource extends KeyValueFsStreamableSource<Long, String> {
	
	/**
	 * @param path
	 */
	private TextSource(Path... path) {
		super(Long.class, String.class, new Supplier<Path[]>() {
			@Override
			public Path[] get() {
				return path;
			}
		});	
	}
	
	/**
	 * @param sourceSupplier
	 */
	private TextSource(Supplier<Path[]> sourceSupplier) {
		super(Long.class, String.class, sourceSupplier);	
	}
	
	/**
	 * Factory method to construct TextSource from the array of provided paths.
	 * 
	 * @param path
	 */
	public static TextSource create(Path... path) {
		Assert.notEmpty(path);
		return new TextSource(path);
	}
	
	public String toString(){
		String superValue = super.toString();
		return superValue + " key/value:[Long/String];"; 
	}
	
	/**
	 * Factory method to construct TextSource using a provided {@link Supplier}.
	 * 
	 * @param path
	 */
	public static TextSource create(Supplier<Path[]> sourceSupplier) {
		Assert.notNull(sourceSupplier);
		return new TextSource(sourceSupplier);
	}

	@Override
	public Stream<String> toStream() {
		// TODO Auto-generated method stub
		return null;
	}
}

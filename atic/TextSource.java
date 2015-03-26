package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.dstream.Source;
import org.apache.dstream.utils.Assert;

/**
 * Implementation of {@link ComputableSource} and {@link KeyValueFsStreamableSource} which 
 * represents a typical text file.
 */
public class TextSource extends KeyValueFsSource<Long, String> {
	
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
	 * This method will validate the actual existence of the resources identified by the paths. 
	 * 
	 * @param path
	 */
	public static Source<String> create(Path... path) {
		Assert.notEmpty(path);
		Arrays.stream(path).forEach(p -> {
			try {
				p.getFileSystem().provider().checkAccess(p);
			} catch (Exception e) {
				throw new IllegalStateException("Failed to create TextSource", e);
			}
		});
		return new TextSource(path);
	}
	
	public String toString(){
		String superValue = super.toString();
		return superValue + " key/value:[Long/String]; " + Arrays.asList(this.getPath()); 
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

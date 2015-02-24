package org.apache.dstream.io;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.function.Supplier;

import junit.framework.Assert;

import org.junit.Test;

public class TextSourceTests {

	@Test(expected=IllegalArgumentException.class)
	public void emptyPath() throws Exception {
		TextSource.create();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void nullPath() throws Exception {
		Path[] path = null;
		TextSource.create(path);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void emptyPathArray() throws Exception {
		Path[] path = new Path[]{};
		TextSource.create(path);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void nullSupplier() throws Exception {
		Supplier<Path[]> supplier = null;
		TextSource.create(supplier);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void supplierReturnedNull() throws Exception {
		Supplier<Path[]> pathSupplier = new Supplier<Path[]>() {
			@Override
			public Path[] get() {
				return null;
			}
		};
		Assert.assertNotNull(TextSource.create(pathSupplier));
	}
	
	@Test
	public void validateFactoryWithPath() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Assert.assertNotNull(TextSource.create(path));
	}
	
	@Test
	public void validateFactoryWithMultiplePath() throws Exception {
		Path path1 = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Path path2 = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Assert.assertNotNull(TextSource.create(path1, path2));
	}
	
	@Test
	public void validateFactoryWithSupplier() throws Exception {
		Supplier<Path[]> pathSupplier = new Supplier<Path[]>() {
			Path path1 = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
			Path path2 = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
			@Override
			public Path[] get() {
				return new Path[]{path1, path2};
			}
		};
		Assert.assertNotNull(TextSource.create(pathSupplier));
	}
}

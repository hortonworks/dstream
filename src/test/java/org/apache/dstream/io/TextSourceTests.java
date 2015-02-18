package org.apache.dstream.io;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import junit.framework.Assert;

import org.apache.logging.log4j.util.Strings;
import org.junit.Test;

public class TextSourceTests {

	@Test
	public void validateNonNullInitialValues() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		try {
			TextSource.create(path);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
	}
}

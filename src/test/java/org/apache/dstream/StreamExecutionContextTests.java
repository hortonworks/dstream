package org.apache.dstream;

import java.io.File;
import java.net.URL;

import org.apache.dstream.io.TextFile;
import org.apache.logging.log4j.util.Strings;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class StreamExecutionContextTests {
	
	@Test
	public void validateNullSourceException() throws Exception {
		try {
			StreamExecutionContext.of(null);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
	}
	
	@Test
	public void validateExecutionContextFound() throws Exception {
		URL url = new File("src/test/java/org/apache/dstream/sample.txt").toURI().toURL();
		Object executionContext = StreamExecutionContext.of(TextFile.create(Long.class, String.class, url));
		Assert.assertNotNull(executionContext);
		Assert.assertTrue(executionContext instanceof LocalStreamExecutionContext);
	}
	
}

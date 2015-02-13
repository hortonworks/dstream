package org.apache.dstream;

import java.io.File;
import java.net.URI;

import org.apache.dstream.io.TextSource;
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
		URI uri = new File("src/test/java/org/apache/dstream/sample.txt").toURI();
		Object executionContext = StreamExecutionContext.of(TextSource.create(Long.class, String.class, uri));
		Assert.assertNotNull(executionContext);
		Assert.assertTrue(executionContext instanceof LocalStreamExecutionContext);
	}
	
}

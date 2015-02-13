package org.apache.dstream.io;

import java.io.File;
import java.net.URI;

import junit.framework.Assert;

import org.apache.logging.log4j.util.Strings;
import org.junit.Test;

public class TextSourceTests {

	@Test
	public void validateNonNullInitialValues() throws Exception {
		URI url = new File("src/test/java/org/apache/dstream/sample.txt").toURI();
		try {
			TextSource.create(null, String.class, url);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
		
		try {
			TextSource.create(String.class, null, url);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
		
		try {
			TextSource.create(String.class, String.class, null);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
	}
}

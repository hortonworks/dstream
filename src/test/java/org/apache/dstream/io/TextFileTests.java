package org.apache.dstream.io;

import java.io.File;
import java.net.URL;

import junit.framework.Assert;

import org.apache.logging.log4j.util.Strings;
import org.junit.Test;

public class TextFileTests {

	@Test
	public void validateNonNullInitialValues() throws Exception {
		URL url = new File("src/test/java/org/apache/dstream/sample.txt").toURI().toURL();
		try {
			TextFile.create(null, String.class, url);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
		
		try {
			TextFile.create(String.class, null, url);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
		
		try {
			TextFile.create(String.class, String.class, null);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
	}
}

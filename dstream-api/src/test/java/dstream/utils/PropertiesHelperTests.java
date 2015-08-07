package dstream.utils;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import dstream.DStreamConstants;
import dstream.utils.PropertiesHelper;

public class PropertiesHelperTests {

	@Test(expected=IllegalStateException.class)
	public void missingConfiguration(){
		PropertiesHelper.loadProperties("foo.cfg");
	}
	
	@Test
	public void validConfiguration() {
		Properties prop = PropertiesHelper.loadProperties("DStreamOperationsCollectorTests.cfg");
		assertTrue(prop.containsKey(DStreamConstants.DELEGATE));
		assertTrue(prop.containsKey(DStreamConstants.SOURCE + "foo"));
	}
}

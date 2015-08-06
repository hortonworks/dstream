package org.apache.dstream.utils;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.dstream.DistributableConstants;
import org.apache.dstream.utils.PropertiesHelper;
import org.junit.Test;

public class PropertiesHelperTests {

	@Test(expected=IllegalStateException.class)
	public void missingConfiguration(){
		PropertiesHelper.loadProperties("foo.cfg");
	}
	
	@Test
	public void validConfiguration() {
		Properties prop = PropertiesHelper.loadProperties("DStreamOperationsCollectorTests.cfg");
		assertTrue(prop.containsKey(DistributableConstants.DELEGATE));
		assertTrue(prop.containsKey(DistributableConstants.SOURCE + "foo"));
	}
}

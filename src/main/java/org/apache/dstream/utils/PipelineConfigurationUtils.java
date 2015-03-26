package org.apache.dstream.utils;

import java.io.InputStream;
import java.util.Properties;

public class PipelineConfigurationUtils {

	public static Properties loadDelegatesConfig(){
		return loadConfig("pipeline-delegates.cfg");
	}
	
	public static Properties loadPipelineConfig(String pipelineName){
		return loadConfig(pipelineName + ".cfg");
	}
	
	private static Properties loadConfig(String configName){
		Properties prop = new Properties();
		InputStream is = null;
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try {
			is = cl.getResourceAsStream(configName);
			prop.load(is);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load configuration properties: " + configName, e);
		} finally {
			try {
				is.close();
			} catch (Exception e2) {
				// ignore
			}
		}
		return prop;
	}
}

package org.apache.dstream.utils;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class PipelineConfigurationUtils {

	/**
	 * 
	 * @return
	 */
	public static Properties loadDelegatesConfig(){
		return loadConfig("pipeline-delegates.cfg");
	}
	
	/**
	 * 
	 * @param executionName
	 * @return
	 */
	public static Properties loadExecutionConfig(String executionName){
		try {
			return loadConfig(executionName + ".cfg");
		} 
		catch (Exception e) {
			String message = e.getMessage();
			throw new IllegalStateException(message + ". Possible reason: Configuration file for execution '" 
					+ executionName + "' is not provided at the root of the classpath.", e);
		}
	}
	
	/**
	 * 
	 */
	private static Properties loadConfig(String configName){
		Properties prop = new Properties();
		InputStream is = null;
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try {
			is = cl.getResourceAsStream(configName);
			prop.load(is);
			resolveSystemPropertyPlaceholders(prop);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to load configuration properties: " + configName, e);
		} 
		finally {
			try {
				is.close();
			} catch (Exception e2) {
				// ignore
			}
		}
		return prop;
	}
	
	/**
	 * 
	 */
	private static void resolveSystemPropertyPlaceholders(Properties props){
        Map<String, String> systemProperties = System.getProperties().entrySet()
        		.stream().collect(Collectors.toMap(prop -> "${" + prop.getKey() + "}", prop -> (String)prop.getValue()));
        
        props.entrySet().stream().forEach(executionProp -> systemProperties.entrySet().stream()
        	.filter(systemProp -> ((String) executionProp.getValue()).contains(systemProp.getKey()))
        	.forEach(systemProp -> executionProp.setValue(((String) executionProp.getValue()).replace(systemProp.getKey(), systemProp.getValue()))));
    }
}

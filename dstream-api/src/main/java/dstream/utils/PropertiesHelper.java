package dstream.utils;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Helper class to load properties from files which supports 
 * property place-holders to System properties (e.g., propName =file:${user.dir}/src/test);
 */
public class PropertiesHelper {
	
	/**
	 * 
	 * @param propertyFilePath
	 * @return
	 */
	public static Properties loadProperties(String propertyFilePath){
		Properties prop = new Properties();
		InputStream is = null;
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try {
			is = cl.getResourceAsStream(propertyFilePath);
			Assert.notNull(is, "Failed to obtain InputStream for " + propertyFilePath);
			prop.load(is);
			resolveSystemPropertyPlaceholders(prop);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to load configuration properties: " + propertyFilePath, e);
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

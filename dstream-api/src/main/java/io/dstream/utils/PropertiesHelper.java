/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dstream.utils;

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
	 * Will create an instance of {@link Properties} object loaded from the properties file
	 * identified by the given <i>propertyFilePath</i> relative to the root of the classpath
	 * 
	 * @param propertyFilePath path to the property file.
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

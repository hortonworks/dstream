/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dstream.tez.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.dstream.tez.TezConstants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.TezConfiguration;
/**
 * Utility functions related to variety of tasks to be performed in HADOOP
 * such as setting up LocalResource, provisioning classpath etc.
 *
 */
public class HadoopUtils {
	private static final Log logger = LogFactory.getLog(HadoopUtils.class);

	/**
	 * Creates {@link LocalResource}s based on the current user's classpath
	 * 
	 * @param fs
	 * @param classPathDir
	 * @return
	 */
	public static Map<String, LocalResource> createLocalResources(FileSystem fs, String classPathDir) {
		Map<String, LocalResource> localResources = provisionAndLocalizeCurrentClasspath(fs, classPathDir);
		return localResources;
	}
	
	/**
	 * Provisions resource represented as {@link File} to the {@link FileSystem} for a given application
	 * 
	 * @param localResource
	 * @param fs
	 * @param applicationName
	 * @return
	 */
	public static Path provisionResourceToFs(File localResource, FileSystem fs, String applicationName) throws Exception {
		String destinationFilePath = applicationName + "/" + localResource.getName();
		Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
		provisioinResourceToFs(fs, new Path(localResource.getAbsolutePath()), provisionedPath);
		return provisionedPath;
	}
	
	/**
	 * Creates a single {@link LocalResource} for the provisioned resource identified with {@link Path}
	 * 
	 * @param fs
	 * @param provisionedResourcePath
	 * @return
	 */
	public static LocalResource createLocalResource(FileSystem fs, Path provisionedResourcePath){
		try {
			FileStatus scFileStatus = fs.getFileStatus(provisionedResourcePath);
			LocalResource localResource = LocalResource.newInstance(
					ConverterUtils.getYarnUrlFromURI(provisionedResourcePath.toUri()),
					LocalResourceType.FILE,
					LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
					scFileStatus.getModificationTime());
			return localResource;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to communicate with FileSystem while creating LocalResource: " + fs, e);
		}
	}
	
	/**
	 * Will provision current classpath to YARN and return an array of 
	 * {@link Path}s representing provisioned resources
	 * If 'generate-jar' system property is set it will also generate the JAR for the current 
	 * working directory (mainly used when executing from IDE)
	 * 
	 * @return
	 */
	private static Path[] provisionClassPath(FileSystem fs, String applicationName, String[] classPathExclusions){
		String genJarProperty = System.getProperty(TezConstants.GENERATE_JAR);
		boolean generateJar = genJarProperty != null && Boolean.parseBoolean(genJarProperty);
		List<Path> provisionedPaths = new ArrayList<Path>();
		List<File> generatedJars = new ArrayList<File>();
		
		boolean confFromHadoopConfDir = generateConfigJarFromHadoopConfDir(fs, applicationName, provisionedPaths, generatedJars);
		
		TezConfiguration tezConf = new TezConfiguration(fs.getConf());
		boolean provisionTez = true;
		if (tezConf.get("tez.lib.uris") != null){
			provisionTez = false;
		}
		URL[] classpath = ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs();
		for (URL classpathUrl : classpath) {
			File f = new File(classpathUrl.getFile());
			if (f.isDirectory()) {			
				if (generateJar){
					String jarFileName = ClassPathUtils.generateJarFileName("application");
					f = doGenerateJar(f, jarFileName, generatedJars, "application");
				} 
				else if (f.getName().equals("conf") && !confFromHadoopConfDir){
					String jarFileName = ClassPathUtils.generateJarFileName("conf_application");
					f = doGenerateJar(f, jarFileName, generatedJars, "configuration");
				}
				else {
					f = null;
				}
			} 
			if (f != null){
				if (f.getName().startsWith("tez-") && !provisionTez){
					logger.info("Skipping provisioning of " + f.getName() + " since Tez libraries are already provisioned");
					continue;
				}
				String destinationFilePath = applicationName + "/" + f.getName();
				Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
				if (shouldProvision(provisionedPath.getName(), classPathExclusions)){
					try {
						provisioinResourceToFs(fs, new Path(f.getAbsolutePath()), provisionedPath);
						provisionedPaths.add(provisionedPath);
					} catch (Exception e) {
						logger.warn("Failed to provision " + provisionedPath + "; " + e.getMessage());
						if (logger.isDebugEnabled()){
							logger.trace("Failed to provision " + provisionedPath, e);
						}
					}
				}
			}
			
		}
		
		for (File generatedJar : generatedJars) {
			try {
				generatedJar.delete(); 
			} catch (Exception e) {
				logger.warn("Failed to delete generated jars", e);
			}
		}
		return provisionedPaths.toArray(new Path[]{});
	}
	
	/**
	 * 
	 * @param f
	 * @param jarFileName
	 * @param generatedJars
	 * @return
	 */
	private static File doGenerateJar(File f, String jarFileName, List<File> generatedJars, String subMessage) {
		if (logger.isDebugEnabled()){
			logger.debug("Generating " + subMessage + " JAR: " + jarFileName);
		}
		File jarFile = ClassPathUtils.toJar(f, jarFileName);
		generatedJars.add(jarFile);
		return jarFile;
	}
	
	/**
	 * 
	 */
	private static boolean generateConfigJarFromHadoopConfDir(FileSystem fs, String applicationName, List<Path> provisionedPaths, List<File> generatedJars){
		boolean generated = false;
		String hadoopConfDir = System.getenv().get("HADOOP_CONF_DIR");
		if (hadoopConfDir != null && hadoopConfDir.trim().length() > 0){
			String jarFileName = ClassPathUtils.generateJarFileName("conf_");
			File confDir = new File(hadoopConfDir.trim());
			File jarFile = doGenerateJar(confDir, jarFileName, generatedJars, "configuration (HADOOP_CONF_DIR)");
			String destinationFilePath = applicationName + "/" + jarFile.getName();
			Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
			
			try {
				provisioinResourceToFs(fs, new Path(jarFile.getAbsolutePath()), provisionedPath);
				provisionedPaths.add(provisionedPath);
				generated = true;
			} catch (Exception e) {
				logger.warn("Failed to provision " + provisionedPath + "; " + e.getMessage());
				if (logger.isDebugEnabled()){
					logger.warn("Failed to provision " + provisionedPath, e);
				}	
				throw new IllegalStateException(e);
			}
		}
		
		String tezConfDir = System.getenv().get("TEZ_CONF_DIR");
		if (tezConfDir != null && tezConfDir.trim().length() > 0){
			String jarFileName = ClassPathUtils.generateJarFileName("conf_tez");
			File confDir = new File(tezConfDir.trim());
			File jarFile = doGenerateJar(confDir, jarFileName, generatedJars, "configuration (TEZ_CONF_DIR)");
			
			try {
				URLClassLoader cl = (URLClassLoader) ClassLoader.getSystemClassLoader();
				Method m = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
				m.setAccessible(true);
				m.invoke(cl, jarFile.toURI().toURL());
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
			
			String destinationFilePath = applicationName + "/" + jarFile.getName();
			Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
			
			try {
				provisioinResourceToFs(fs, new Path(jarFile.getAbsolutePath()), provisionedPath);
				provisionedPaths.add(provisionedPath);
				generated = true;
			} catch (Exception e) {
				logger.warn("Failed to provision " + provisionedPath + "; " + e.getMessage());
				if (logger.isDebugEnabled()){
					logger.warn("Failed to provision " + provisionedPath, e);
				}
				throw new IllegalStateException(e);
			}
			
		}
		return generated;
	}
	
	/**
	 * 
	 * @param path
	 * @param classPathExclusions
	 * @return
	 */
	private static boolean shouldProvision(String path, String[] classPathExclusions){
		if (classPathExclusions != null){
			for (String exclusion : classPathExclusions) {
				if (path.contains(exclusion) || !path.endsWith(".jar")){
					if (logger.isDebugEnabled()){
						logger.debug("Excluding resource: " + path);
					}
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * 
	 * @param fs
	 * @param sourcePath
	 * @param destPath
	 * @param appId
	 * @param applicationName
	 * @return
	 */
	private static Map<String, LocalResource> createLocalResources(FileSystem fs, Path[] provisionedResourcesPaths) {
		Map<String, LocalResource> localResources = new LinkedHashMap<String, LocalResource>();
		for (Path provisionedResourcesPath : provisionedResourcesPaths) {
			LocalResource localResource = createLocalResource(fs, provisionedResourcesPath);
			localResources.put(provisionedResourcesPath.getName(), localResource);
		}
		return localResources;
	}

	/**
	 * 
	 * @param fs
	 * @param sourcePath
	 * @param destPath
	 */
	private static synchronized void provisioinResourceToFs(FileSystem fs, Path sourcePath, Path destPath) throws Exception {
		if (logger.isDebugEnabled()){
			logger.debug("Provisioning '" + sourcePath + "' to " + destPath);
		}
		if (!fs.exists(destPath)){
			fs.copyFromLocalFile(sourcePath, destPath);
		}
		else {
			logger.debug("Skipping provisioning of " + destPath + " since it already exists.");
		}
	}
	/**
	 * 
	 */
	private static Map<String, LocalResource> provisionAndLocalizeCurrentClasspath(FileSystem fs, String appName) {
		Path[] provisionedResourcesPaths = HadoopUtils.provisionClassPath(fs, appName, ClassPathUtils.initClasspathExclusions(TezConstants.CLASSPATH_EXCLUSIONS));
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, provisionedResourcesPaths);

		return localResources;
	}
}

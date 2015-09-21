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
package org.apache.dstream.tez.utils;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.dstream.utils.Assert;
import io.dstream.utils.SerializationUtils;

public class HdfsSerializerUtils {

	/**
	 * Will serialize object to HDFS returning its {@link Path}.
	 *
	 * @param source
	 * @param fs
	 * @param targetPath
	 * @return
	 */
	public static Path serialize(Object source, FileSystem fs, Path targetPath) {
		Assert.notNull(targetPath, "'targetPath' must not be null");
		Assert.notNull(fs, "'fs' must not be null");
		Assert.notNull(source, "'source' must not be null");

		Path resultPath = targetPath.makeQualified(fs.getUri(), fs.getWorkingDirectory());
		OutputStream targetOutputStream = null;
		try {
			targetOutputStream = fs.create(targetPath);
			SerializationUtils.serialize(source, targetOutputStream);
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to serialize " + source + " to " + resultPath, e);
		}
		return resultPath;
	}

	/**
	 *
	 * @param sourcePath
	 * @param fs
	 * @param resultType
	 * @return
	 */
	public static  <T> T deserialize(Path sourcePath, FileSystem fs, Class<T> resultType) {
		Assert.notNull(sourcePath, "'sourcePath' must not be null");
		Assert.notNull(fs, "'fs' must not be null");
		Assert.notNull(resultType, "'resultType' must not be null");

		InputStream sourceInputStream = null;
		try {
			sourceInputStream = fs.open(sourcePath);
			T result = SerializationUtils.deserialize(sourceInputStream, resultType);
			return result;
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to de-serialize from " + sourcePath + " object type " + resultType, e);
		}
	}
}

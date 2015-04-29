package org.apache.dstream.tez.utils;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.SerializationUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

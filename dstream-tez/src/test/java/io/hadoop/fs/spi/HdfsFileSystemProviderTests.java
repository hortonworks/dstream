package io.hadoop.fs.spi;

import java.nio.file.spi.FileSystemProvider;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import io.dstream.hadoop.fs.spi.HdfsFileSystemProvider;

public class HdfsFileSystemProviderTests {

	@Test
	public void validateProviderLoaded() throws Exception {
		List<FileSystemProvider> providers = FileSystemProvider.installedProviders();
		int length = providers.stream().filter(x -> x instanceof HdfsFileSystemProvider).toArray().length;
		Assert.assertEquals(1, length);
		//FileSystem fs = FileSystems.getFileSystem(new URI("hdfs://hortonworks.com:9000"));
	}
}

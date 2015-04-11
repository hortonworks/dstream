package org.apache.hadoop.fs.spi;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;


class HdfsFileSystem extends FileSystem {
	private final org.apache.hadoop.fs.FileSystem hdfs;
	
	private final HdfsFileSystemProvider provider;
	
	private final Configuration hdfsConfiguration;
	
	public HdfsFileSystem(HdfsFileSystemProvider provider, Configuration hdfsConfiguration){
		this.provider = provider;
		this.hdfsConfiguration = hdfsConfiguration;
		try {
			this.hdfs = org.apache.hadoop.fs.FileSystem.get(this.hdfsConfiguration);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to create an instance of delegate file system for HDFS");
		}
	}

	@Override
	public FileSystemProvider provider() {
		return this.provider;
	}

	@Override
	public void close() throws IOException {
		this.hdfs.close();
	}

	@Override
	public boolean isOpen() {
		return false;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public String getSeparator() {
		return "/";
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path getPath(String first, String... more) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(first);
		for (String segment : more) {
			buffer.append("/");
			buffer.append(segment);
		}
		org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(buffer.toString());
		return new HdfsPath(hdfsPath, this);
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WatchService newWatchService() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public boolean exists(org.apache.hadoop.fs.Path hdfsPath){
		try {
			return hdfs.exists(hdfsPath);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to verufy if file '" + hdfsPath + "' exists", e);
		}
	}
	
	public org.apache.hadoop.fs.FileSystem getRootFileSystem(){
		return this.hdfs;
	}

}

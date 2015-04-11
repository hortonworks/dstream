package org.apache.hadoop.fs.spi;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileSystemProvider extends FileSystemProvider {
	
	private final Logger logger = LoggerFactory.getLogger(HdfsFileSystemProvider.class);
	
	private final Configuration hdfsConfiguration;

	private URI uri;
	
	private Map<String, ?> env;
	
	private HdfsFileSystem hdfs;
	
	public HdfsFileSystemProvider(){
		this.hdfsConfiguration = new Configuration();
		this.hdfs = new HdfsFileSystem(this, this.hdfsConfiguration);
		
		if (logger.isInfoEnabled()){
			logger.info("Linking to HDFS FileSystem @: " + this.hdfsConfiguration.get("fs.defaultFS"));
		}
	}
	
	@Override
	public String getScheme() {
		
		return "hdfs";
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		this.uri = uri;
		this.env = env;
		return this.hdfs;
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		try {
			return this.newFileSystem(uri, new HashMap<String, Object>());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Path getPath(URI uri) {
		return null;
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path,
			Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir,
			Filter<? super Path> filter) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Path path) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void copy(Path source, Path target, CopyOption... options)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void move(Path source, Path target, CopyOption... options)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		// TODO Auto-generated method stub
		org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(path.toUri());
		if (!this.hdfs.exists(hdfsPath)){
			throw new FileNotFoundException("File: " + path + " does not exist");
		}
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path,
			Class<V> type, LinkOption... options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path,
			Class<A> type, LinkOption... options) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes,
			LinkOption... options) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value,
			LinkOption... options) throws IOException {
		// TODO Auto-generated method stub

	}

}

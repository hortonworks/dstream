package org.apache.dstream.io;

import java.nio.file.Path;

import org.apache.dstream.Source;

public interface FsSource<T> extends Source<T> {
	
	public abstract Path[] getPath();
	
	public abstract String getScheme();
}

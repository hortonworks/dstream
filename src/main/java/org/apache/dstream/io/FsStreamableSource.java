package org.apache.dstream.io;

import java.nio.file.Path;

public interface FsStreamableSource<T> extends StreamSource<T> {
	
	public abstract Path[] getPath();
	
	public abstract String getScheme();
}

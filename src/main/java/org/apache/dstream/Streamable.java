package org.apache.dstream;

import java.net.URL;
import java.util.stream.Stream;

public interface Streamable<T> {

	public Stream<T> toStream();
	
	public URL getUrl();
}

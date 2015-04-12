package org.apache.dstream.builder;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

class TextFileStreamBuilder {

	/**
	 * 
	 * @param uri
	 * @return
	 */
	public Stream<String> toStream(URI uri) {
		try {
			Stream<String> lines = Files.lines(Paths.get(uri));
			return lines; 
		} catch (Exception e) {
			throw new IllegalStateException("Failed to create Stream from " + uri, e);
		}
	}
}

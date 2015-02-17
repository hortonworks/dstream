package org.apache.dstream.local;

import java.nio.file.Path;
import java.util.Objects;

import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.io.StreamableSource;

public class OutputSpecificationImpl implements OutputSpecification {
	
	private final Path path;
	
	public OutputSpecificationImpl(Path path){
		Objects.requireNonNull(path, "'path' must not be null");
		
		this.path = path;
	}
	
	public static OutputSpecificationImpl create(Path path){
		return new OutputSpecificationImpl(path);
	}

	@Override
	public Path getOutputPath() {
		return this.path;
	}

	@Override
	public <T> StreamableSource<T> toStreamableSource() {
		// TODO Auto-generated method stub
		return null;
	}

}

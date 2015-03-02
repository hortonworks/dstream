package org.apache.dstream;

import java.nio.file.Path;
import java.util.Objects;

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
	public <T> DistributableSource<T> toStreamableSource() {
		// TODO Auto-generated method stub
		return null;
	}

}

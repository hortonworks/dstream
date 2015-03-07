package org.apache.dstream;

import java.nio.file.Path;
import java.util.Objects;

public class SimpleOutputSpecification implements OutputSpecification {
	
	private final Path path;
	
	public SimpleOutputSpecification(Path path){
		Objects.requireNonNull(path, "'path' must not be null");
		
		this.path = path;
	}
	
	public static SimpleOutputSpecification create(Path path){
		return new SimpleOutputSpecification(path);
	}

	@Override
	public Path getOutputPath() {
		return this.path;
	}

	@Override
	public <T> DataPipeline<T> toStreamableSource() {
		// TODO Auto-generated method stub
		return null;
	}

}

package org.apache.dstream;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PipelineExecutionDelegate {
	
	private final Logger logger = LoggerFactory.getLogger(PipelineExecutionDelegate.class);
	
	private final PipelineSpecification pipelineSpecification;
	
	public PipelineExecutionDelegate(PipelineSpecification pipelineSpecification){
		this.pipelineSpecification = pipelineSpecification;
	}
	
	public Stream<?> execute() {
		if (logger.isInfoEnabled()){
			logger.info("Executing pipeline: " + pipelineSpecification);
		}
		
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(this.pipelineSpecification);
			oos.close();
			System.out.println("Size: " + bos.toByteArray().length);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}

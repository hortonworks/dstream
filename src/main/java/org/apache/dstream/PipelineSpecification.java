package org.apache.dstream;

import java.io.Serializable;
import java.util.List;

public interface PipelineSpecification extends Serializable {
	
	public String getName();

	public List<Object> getSources();
	
	public List<Stage> getStages();
	
	public abstract class Stage implements Serializable {
		private static final long serialVersionUID = 4321682502843990767L;
		public abstract String getName();
		public abstract int getId();
		public abstract Object getProcessingInstruction();		
		public String toString() {
			return this.getName() + ":" + this.getId() + ":" + (this.getProcessingInstruction() instanceof Processor ? "(proc)" : "(func)");
		}
	}
}

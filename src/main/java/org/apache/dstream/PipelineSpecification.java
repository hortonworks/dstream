package org.apache.dstream;

import java.io.Serializable;
import java.util.List;

import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.support.DefaultHashPartitioner;
import org.apache.dstream.support.Partitioner;

public interface PipelineSpecification extends Serializable {
	
	public String getName();

	public List<Stage> getStages();
	
	public abstract class Stage implements Serializable {
		private static final long serialVersionUID = 4321682502843990767L;
		public abstract String getName();
		public abstract int getId();
		public abstract Function<?, ?> getProcessingFunction();	
		public abstract SourceSupplier<?> getSourceSupplier();	
		public abstract Class<?> getSourceItemType();
		public Partitioner<? extends Object> getPartitioner(){
			return new DefaultHashPartitioner<>(1);
		}
		public String toString() {
			return this.getId() + ":" + this.getName() + (getSourceSupplier() != null ? getSourceSupplier() : "");
		}
	}
}

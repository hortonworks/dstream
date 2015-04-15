package org.apache.dstream;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.support.DefaultHashPartitioner;
import org.apache.dstream.support.Partitioner;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceSupplier;

/**
 * 
 *
 */
public interface DistributablePipelineSpecification extends Serializable {
	
	public String getName();

	public List<Stage> getStages();
	
	/**
	 * 
	 *
	 */
	public abstract class Stage implements Serializable {
		private static final long serialVersionUID = 4321682502843990767L;
		public abstract BinaryOperator<?> getAggregatorOperator();
		public abstract String getName();
		public abstract int getId();
		public abstract Function<Stream<?>, Stream<?>> getProcessingFunction();	
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

package org.apache.dstream;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.support.DefaultHashPartitioner;
import org.apache.dstream.support.Partitioner;
import org.apache.dstream.support.SerializableFunctionConverters.BiFunction;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Pair;

/**
 * 
 *
 */
public interface ExecutionContextSpecification extends Serializable {
	
	public String getName();

	public List<Stage> getStages();
	
	public URI getOutputUri();
	
	/**
	 * 
	 */
	public abstract class Stage implements Serializable {
		private static final long serialVersionUID = 4321682502843990767L;
		
		private Function<Stream<?>, Stream<?>> processingFunction;
		
		private SourceSupplier<?> sourceSupplier;
		
		private ExecutionContextSpecification dependentexecutionContextSpec;
		
		private BiFunction<Stream<?>, Stream<?>, Stream<?>> mergeFunction;
		

		public abstract BinaryOperator<Object> getAggregatorOperator();
		
		public abstract String getName();
		
		public abstract int getId();
		
		public Function<Stream<?>, Stream<?>> getProcessingFunction(){
			return this.processingFunction;
		}
		
		public SourceSupplier<?> getSourceSupplier(){
			return this.sourceSupplier;
		}
		
		public abstract Class<?> getSourceItemType();
		
		public Partitioner<? extends Object> getPartitioner(){
			return new DefaultHashPartitioner<>(1);
		}
		
		public String toString() {
			return this.getId() + ":" + this.getName() + (getSourceSupplier() != null ? getSourceSupplier() : "");
		}
		
		protected void setProcessingFunction(Function<Stream<?>, Stream<?>> processingFunction){
			Assert.isTrue(processingFunction instanceof Serializable, "'processingFunction' is not Serializable");
			this.processingFunction = processingFunction;
		}
		
		protected void setSourceSupplier(SourceSupplier<?> sourceSupplier){
			this.sourceSupplier = sourceSupplier;
		}
		
		public Pair<ExecutionContextSpecification, BiFunction<Stream<?>, Stream<?>, Stream<?>>> getDependentExecutionContextSpec() {
			return Pair.of(this.dependentexecutionContextSpec, this.mergeFunction);
		}

		public void setDependentExecutionContextSpec(ExecutionContextSpecification dependentexecutionContextSpec,
				BiFunction<Stream<?>, Stream<?>, Stream<?>> mergeFunction) {
			this.dependentexecutionContextSpec = dependentexecutionContextSpec;
			this.mergeFunction = mergeFunction;
		}
	}
}

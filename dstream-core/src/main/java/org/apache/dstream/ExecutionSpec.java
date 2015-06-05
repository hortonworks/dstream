package org.apache.dstream;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.support.DefaultHashPartitioner;
import org.apache.dstream.support.Partitioner;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;

/**
 * Strategy to represent execution specification of an <i>execution pipeline</i>.<br>
 * The <i>execution pipeline</i> is an instance of any {@link DistributableExecutable} 
 * on which {@link DistributableExecutable#executeAs(String)} operation has been invoked.
 *
 */
public interface ExecutionSpec {
	
	public String getName();

	public List<Stage> getStages();
	
	public URI getOutputUri();
	
	/**
	 * Contains attributes related to individual execution stages of <i>execution pipeline</i>.
	 */
	public abstract class Stage {
		private final List<String> operations = new ArrayList<String>();
		
		private Function<Stream<?>, Stream<?>> processingFunction;
		
		private SourceSupplier<?> sourceSupplier;
		
		private ExecutionSpec dependentExecutionSpec;

		public abstract BinaryOperator<Object> getAggregatorOperator();
		
		public abstract String getName();
		
		public abstract int getId();
		
		public Function<Stream<?>, Stream<?>> getProcessingFunction(){
			return this.processingFunction;
		}
		
		public void addOperationName(String operationName){
			this.operations.add(operationName);
		}
		
		public String getOperationNames(){
			return this.operations.toString();
		}
		
		public SourceSupplier<?> getSourceSupplier(){
			return this.sourceSupplier;
		}
		
		public abstract Class<?> getSourceItemType();
		
		public Partitioner<? extends Object> getPartitioner(){
			return new DefaultHashPartitioner<>(1);
		}
		
		public String toString() {
			return this.getName() + 
					(this.getDependentExecutionSpec() == null ? "" : this.getDependentExecutionSpec().getStages());
		}
		
		protected void setProcessingFunction(Function<Stream<?>, Stream<?>> processingFunction){
			Assert.isTrue(processingFunction instanceof Serializable, "'processingFunction' is not Serializable");
			this.processingFunction = processingFunction;
		}
		
		protected void setSourceSupplier(SourceSupplier<?> sourceSupplier){
			this.sourceSupplier = sourceSupplier;
		}
		
		public ExecutionSpec getDependentExecutionSpec() {
			return this.dependentExecutionSpec;
		}

		public void setDependentExecutionSpec(ExecutionSpec dependentExecutionSpec) {
			this.dependentExecutionSpec = dependentExecutionSpec;
		}
	}
}

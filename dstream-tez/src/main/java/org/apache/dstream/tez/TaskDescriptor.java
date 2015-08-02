package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.function.PartitionerFunction;
import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Supplier;

/**
 * 
 *
 */
public class TaskDescriptor {
	
	private Function<Stream<?>, Stream<?>> function;

	private PartitionerFunction<? super Object> partitioner;
	
	private String name;
	
	private final int id;
	
	
	private String operationName;

	private Class<?> sourceElementType;
	
	private Supplier<?> sourceSupplier;
	
	private List<List<TaskDescriptor>> dependentTasksChains;

	private Class<?> inputFormatClass;
	
	public TaskDescriptor(int id, String name, String operationName){
		this.name = name;
		this.id = id;
		this.operationName = operationName;
	}
	
	public List<List<TaskDescriptor>> getDependentTasksChains() {
		return this.dependentTasksChains;
	}
	
	public String getOperationName() {
		return operationName;
	}

	public void addDependentTasksChain(List<TaskDescriptor> dependentTasksChain) {
		if (this.dependentTasksChains == null){
			this.dependentTasksChains = new ArrayList<>();
		}
		this.dependentTasksChains.add(dependentTasksChain);
	}
	
	public Class<?> getInputFormatClass() {
		return inputFormatClass;
	}

	public void setInputFormatClass(Class<?> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}
	
	public Supplier<?> getSourceSupplier() {
		return sourceSupplier;
	}

	public void setSourceSupplier(Supplier<?> sourceSupplier) {
		this.sourceSupplier = sourceSupplier;
	}
	
	public int getId() {
		return id;
	}

	public PartitionerFunction<? super Object> getPartitioner() {
		return partitioner;
	}

	public void setPartitioner(PartitionerFunction<? super Object> splitter) {
		this.partitioner = splitter;
	}
	
	public Function<Stream<?>, Stream<?>> getFunction() {
		return this.function;
	}
	
	public void compose(Function<Stream<?>, Stream<?>> cFunction) {
		if (this.function != null){
			this.function = this.function.compose(cFunction);
		}
		else {
			this.function = cFunction;
		}
	}
	
	public void andThen(Function<Stream<?>, Stream<?>> aFunction) {
		if (this.function != null){
			this.function = aFunction.compose(this.function);
		}
		else {
			this.function = aFunction;
		}
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public Class<?> getSourceElementType() {
		return sourceElementType;
	}

	public void setSourceElementType(Class<?> sourceElementType) {
		this.sourceElementType = sourceElementType;
	}
}

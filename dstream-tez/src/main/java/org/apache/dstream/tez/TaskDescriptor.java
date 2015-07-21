package org.apache.dstream.tez;

import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.Partitioner;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SerializableFunctionConverters.Supplier;

public class TaskDescriptor {
	
	private Function<Stream<?>, Stream<?>> function;

	private Partitioner<? super Object> partitioner;
	
	private String name;
	
	private final int id;
	
	private Class<?> sourceElementType;
	
	private Supplier<?> sourceSupplier;
	
	private List<TaskDescriptor> dependentTasksChain;

	private Class<?> inputFormatClass;

	public TaskDescriptor(int id){
		this(id, null);
	}
	
	public TaskDescriptor(int id, String name){
		this.name = name;
		this.id = id;
	}
	
	public List<TaskDescriptor> getDependentTasksChain() {
		return dependentTasksChain;
	}

	public void setDependentTasksChain(List<TaskDescriptor> dependentTasksChain) {
		this.dependentTasksChain = dependentTasksChain;
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

	public Partitioner<? super Object> getPartitioner() {
		return partitioner;
	}

	public void setPartitioner(Partitioner<? super Object> splitter) {
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

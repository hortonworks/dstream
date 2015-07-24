package org.apache.dstream.tez;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.function.SerializableFunctionConverters.Supplier;
import org.apache.dstream.support.Partitioner;
import org.apache.dstream.utils.Tuples.Tuple2;

/**
 * 
 *
 */
public class TaskDescriptor {
	
	private Function<Stream<?>, Stream<?>> function;

	private Partitioner<? super Object> partitioner;
	
	private String name;
	
	private final int id;
	
	
	private final String shuffleOperationName;
	

	private Class<?> sourceElementType;
	
	private Supplier<?> sourceSupplier;
	
	private List<Tuple2<Predicate<?>, List<TaskDescriptor>>> dependentTasksChains;

	private Class<?> inputFormatClass;
	
	public TaskDescriptor(int id, String name, String shuffleOperationName){
		this.name = name;
		this.id = id;
		this.shuffleOperationName = shuffleOperationName;
	}
	
	public List<Tuple2<Predicate<?>, List<TaskDescriptor>>> getDependentTasksChains() {
		return this.dependentTasksChains;
	}
	
	public String getShuffleOperationName() {
		return shuffleOperationName;
	}

	public void addDependentTasksChain(Tuple2<Predicate<?>, List<TaskDescriptor>> dependentTasksChain) {
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

	public Partitioner<? super Object> getPartitioner() {
		return partitioner;
	}

	public void setPartitioner(Partitioner<? super Object> splitter) {
		this.partitioner = splitter;
	}
	
	public Function<Stream<?>, Stream<?>> getFunction() {
		this.materializeJoinFunction();

		return this.function;
	}
	
	public void compose(Function<Stream<?>, Stream<?>> cFunction) {
		this.materializeJoinFunction();
		if (this.function != null){
			this.function = this.function.compose(cFunction);
		}
		else {
			this.function = cFunction;
		}
	}
	
	public void andThen(Function<Stream<?>, Stream<?>> aFunction) {
		this.materializeJoinFunction();
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void materializeJoinFunction(){
		if (this.function == null && this.shuffleOperationName.equals("join")){
			Predicate<?>[] predicates = this.dependentTasksChains.stream().map(s -> s._1).collect(Collectors.toList()).toArray(new Predicate[]{});
			
			Function f = new TezJoiner(predicates);
			this.function = f;
		}
	}
}

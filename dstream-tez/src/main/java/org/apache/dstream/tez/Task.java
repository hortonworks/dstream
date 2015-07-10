package org.apache.dstream.tez;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.Partitioner;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Task implements Serializable {

	private static final long serialVersionUID = -1800812882885490376L;

	private final Function<Stream<?>, Stream<?>> function;

	private final Partitioner<? super Object> partitioner;
	
	private final String name;
	
	private final int id;
	
	private Task(int id, String name, Partitioner<? super Object> partitioner, Function<Stream<?>, Stream<?>> function){
		this.id = id;
		this.name = name;
		this.partitioner = partitioner;
		this.function = function;
	}
	
	static Task build(TaskDescriptor taskDescriptor) {
		Function<Stream<?>, Stream<?>> taskFunction = adjustTaskFunction(taskDescriptor);
		Task task = new Task(taskDescriptor.getId(), taskDescriptor.getName(), taskDescriptor.getPartitioner(), taskFunction);
		return task;
	}
	
	public Function<Stream<?>, Stream<?>> getFunction() {
		return function;
	}

	public Partitioner<? super Object> getPartitioner() {
		return partitioner;
	}

	public String getName() {
		return name;
	}

	public int getId() {
		return id;
	}
	
	/**
	 * This will adjust task function to ensure that it is compatible with Hadoop KV readers.
	 * For example, reading Text file Tez will produce KV pairs (offset, line), while user is only expected the value.
	 */
	@SuppressWarnings("rawtypes")
	private static Function<Stream<?>, Stream<?>> adjustTaskFunction(TaskDescriptor taskDescriptor){
		Function<Stream<?>, Stream<?>> modifiedFunction = taskDescriptor.getFunction();
		if (taskDescriptor.getId() == 0 && !Entry.class.isAssignableFrom(taskDescriptor.getSourceElementType())){	
			if (Writable.class.isAssignableFrom(taskDescriptor.getSourceElementType())){
				modifiedFunction = modifiedFunction.compose(stream -> stream.map(s -> ((Entry)s).getValue()));
			} 
			else {
				ParameterizedType parameterizedType = (ParameterizedType) taskDescriptor.getInputFormatClass().getGenericSuperclass();
				Type type = parameterizedType.getActualTypeArguments()[1];
				
				if (Text.class.getName().equals(type.getTypeName())){
					modifiedFunction = modifiedFunction.compose(stream -> stream.map( s -> {
						return ((Entry)s).getValue().toString();}));
				} 
				else {
					//TODO need to design some type of extensible converter to support multiple types of Writable
					throw new IllegalStateException("Can't determine modified function");
				}
			}
		}
		return modifiedFunction;
	}
}

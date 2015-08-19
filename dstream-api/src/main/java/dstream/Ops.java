package dstream;

public enum Ops {
	aggregateValues, 
	classify,
	compute,
	extract,
	filter,
	flatMap,
	join,
	load,
	map,
	mapKeyValues,
	on,
	reduceValues,
	union,
	unionAll;
	
	public static boolean isTransformation(String operation){
		return isTransformation(Ops.valueOf(operation));
	}
	
	public static boolean isShuffle(String operation){
		return isShuffle(Ops.valueOf(operation));
	}
	
	public static boolean isTransformation(Ops operation){
		return operation.equals(flatMap) ||
			   operation.equals(map) ||
			   operation.equals(filter) ||
			   operation.equals(compute);
	}
	
	public static  boolean isShuffle(Ops operation){
		return operation.equals(reduceValues) ||
			   operation.equals(aggregateValues) ||
			   operation.equals(join) ||
			   operation.equals(union) ||
			   operation.equals(unionAll) ||
			   operation.equals(classify);
	}
}

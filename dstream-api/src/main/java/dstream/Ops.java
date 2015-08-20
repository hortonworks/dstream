package dstream;

public enum Ops {
	aggregateValues, 
	classify,
	compute,
	count,
	distinct,
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
	
	public static boolean isTransformation(String operationName){
		return isTransformation(Ops.valueOf(operationName));
	}
	
	public static boolean isShuffle(String operationName){
		return isShuffle(Ops.valueOf(operationName));
	}
	
	public static boolean isStreamTerminal(String operationName){
		return isStreamTerminal(Ops.valueOf(operationName));
	}
	
	public static boolean isStreamTerminal(Ops operation){
		return operation.equals(count);
	}
	
	public static boolean isTransformation(Ops operation){
		return operation.equals(flatMap) ||
			   operation.equals(map) ||
			   operation.equals(filter) ||
			   operation.equals(compute) ||
			   operation.equals(distinct);
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

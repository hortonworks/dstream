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
	max, 
	min,
	on,
	reduce,
	reduceValues,
	sorted,
	union,
	unionAll;
	
	/**
	 * 
	 * @param operationName
	 * @return
	 */
	public static boolean isTransformation(String operationName){
		return isTransformation(Ops.valueOf(operationName));
	}
	
	/**
	 * 
	 * @param operationName
	 * @return
	 */
	public static boolean isShuffle(String operationName){
		return isShuffle(Ops.valueOf(operationName));
	}
	
	/**
	 * 
	 * @param operationName
	 * @return
	 */
	public static boolean isStreamReduce(String operationName){
		return isStreamReduce(Ops.valueOf(operationName));
	}
	
	/**
	 * 
	 * @param operationName
	 * @return
	 */
	public static boolean isStreamComparator(String operationName){
		return isStreamComparator(Ops.valueOf(operationName));
	}
	
	/**
	 * 
	 * @param operation
	 * @return
	 */
	public static boolean isStreamComparator(Ops operation){
		return operation.equals(min) ||
			   operation.equals(max) ||
			   operation.equals(distinct) ||
			   operation.equals(sorted);
	}
	
	/**
	 * 
	 * @param operation
	 * @return
	 */
	public static boolean isStreamReduce(Ops operation){
		return operation.equals(count) ||
			   operation.equals(reduce);
	}
	
	/**
	 * 
	 * @param operation
	 * @return
	 */
	public static boolean isTransformation(Ops operation){
		return operation.equals(flatMap) ||
			   operation.equals(map) ||
			   operation.equals(filter) ||
			   operation.equals(compute);
	}
	
	/**
	 * 
	 * @param operation
	 * @return
	 */
	public static  boolean isShuffle(Ops operation){
		return operation.equals(reduceValues) ||
			   operation.equals(aggregateValues) ||
			   operation.equals(join) ||
			   operation.equals(union) ||
			   operation.equals(unionAll) ||
			   operation.equals(classify);
	}
}

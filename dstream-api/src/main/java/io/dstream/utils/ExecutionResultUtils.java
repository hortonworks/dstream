package io.dstream.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ExecutionResultUtils {

	/**
	 * Prints the resultPartitionsStream to the standard out
	 * 
	 * @param resultPartitionsStream
	 */
	public static <T> void printResults(Stream<Stream<T>> resultPartitionsStream){
		printResults(resultPartitionsStream, false);
	}
	
	/**
	 * Prints the resultPartitionsStream to the standard out, also printing partition separator at the 
	 * start of each result partition if 'printPartitionSeparator' is set to <i>true</i>.
	 * 
	 * @param resultPartitionsStream
	 * @param printPartitionSeparator
	 */
	public static <T> void printResults(Stream<Stream<T>> resultPartitionsStream, boolean printPartitionSeparator){
		AtomicInteger partitionCounter = new AtomicInteger();
		resultPartitionsStream.forEach(resultPartition -> {
			if (printPartitionSeparator){
				System.out.println("\n=> PARTITION:" + partitionCounter.getAndIncrement());
			}
			
			resultPartition.forEach(System.out::println);
		});
	}
}

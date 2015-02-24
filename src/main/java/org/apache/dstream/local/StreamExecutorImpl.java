package org.apache.dstream.local;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.apache.dstream.MergerImpl;
import org.apache.dstream.assembly.Stage;
import org.apache.dstream.assembly.StreamAssembly;
import org.apache.dstream.exec.StreamExecutor;
import org.apache.dstream.io.ListStreamableSource;
import org.apache.dstream.io.ListStreamableSourceTests;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.io.TextSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulator of the distributed execution environment which will utilize Java threads to 
 * parallelize processing. Not intended for performance testing, although natural performance 
 * improvements could be observed due to multi-threading especially in the compute intensive processes.
 * 
 *
 * @param <T>
 */
public class StreamExecutorImpl<T> extends StreamExecutor<T> {

	private final Logger logger = LoggerFactory.getLogger(StreamExecutorImpl.class);
	
	private ExecutorService executor;
	
	public StreamExecutorImpl(StreamAssembly streamAssembly) {
		super(streamAssembly);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<T> execute() {
		if (logger.isInfoEnabled()){
			logger.info("Executing " + this.streamAssembly.getJobName());
		}
		
		StreamableSource<T> source = (StreamableSource<T>) this.streamAssembly.getSource();

		this.executor = Executors.newCachedThreadPool();
		
		Map<Integer, ConcurrentHashMap> partitions = null;
		for (Stage stage : streamAssembly) {
			int partitionSize = ((MergerImpl)stage.getMerger()).getPartitionSize();
			
			partitions = new HashMap<Integer, ConcurrentHashMap>();
			for (int i = 0; i < partitionSize; i++) {
				partitions.put(i, new ConcurrentHashMap());
			}
			
			Split[] splits = null; // splits from source
			for (Split<T> split : splits) {
				this.executor.execute(new Runnable() {
					
					@Override
					public void run() {
						Stream<T> stream = split.toStream();
						/*
						 * - get next stage
						 * - apply mapper function on stream and collect local Map
						 * - write (merge) to concurrent keyValues (apply merge function) based on Partitioner
						 */
					}
				});
			}
			source = null;// create source from partitions. Splits should be = to partition count
		}
		
		/*
		 * - create sharable concurrent Map
		 * - compute tasks (Runnable) where each task will have a Split
		 * - submit task to executor
		 * - wait for completion
		 * - return Stream from Map
		 */
		return null;
	}

	/**
	 * This methods will generate splits from {@link StreamableSource} to mainly emulate the behavior of
	 * the underlying distributed system allowing {@link StreamableSource} to be processed parallelized as if
	 * it was parallelized in such system.
	 * 
	 * @param source
	 */
	private void generateSplits(StreamableSource<T> source){
		if (source instanceof TextSource){
			this.generateTextFileSplits((TextSource) source);
		}
	}
	
	private void generateTextFileSplits(TextSource source) {
		Path[] paths = ((TextSource)source).getPath();
		int splitCount = Runtime.getRuntime().availableProcessors();
		if (paths.length >= splitCount){
			System.out.println("No splitting since file >= splutCount " + splitCount);
		} else {
			long totalLength = 0;
			for (Path path : paths) {
				File file = path.toFile();
				totalLength += file.length();
			}
			long splits = totalLength/splitCount;
			
			for (int i = 0; i < splitCount; i++) {
				
			}
		}
		
	}
}

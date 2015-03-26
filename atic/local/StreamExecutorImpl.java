package org.apache.dstream.local;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.dstream.AbstractDistributable;
import org.apache.dstream.Pipeline;
import org.apache.dstream.DefaultDistributable;
import org.apache.dstream.assembly.Stage;
import org.apache.dstream.assembly.DistributedPipelineAssembly;
import org.apache.dstream.exec.DistributedPipelineExecutor;
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
public class StreamExecutorImpl<T,R> extends DistributedPipelineExecutor<T,R> {

	private final Logger logger = LoggerFactory.getLogger(StreamExecutorImpl.class);
	
	private final ExecutorService executor = Executors.newCachedThreadPool();
	
	public StreamExecutorImpl(DistributedPipelineAssembly<T> streamAssembly) {
		super(streamAssembly);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Pipeline<R> execute() {
//		try {
//			if (logger.isInfoEnabled()){
//				logger.info("Executing " + this.streamAssembly.getJobName());
//			}
//			
//			StreamableSource<T> source = (StreamableSource<T>) this.streamAssembly.getSource();
//		
//			ShuffleWriterImpl finalShuffle = null;
//			
//			for (Stage<T> stage : this.streamAssembly) {
//				Split<T>[] splits = SplitGenerationUtil.generateSplits(source);
//				Assert.notEmpty(splits, "Failed to generate splits from " + source);
//				
//				ShuffleWriterImpl shuffleWriter = this.createShuffleWriter(stage);
//				Task<T, R> task = new Task<T, R>(stage.getStageFunction(), null);
//					
//				AtomicReference<Exception> exception = new AtomicReference<>();
//				CountDownLatch taskCompletionLatch = new CountDownLatch(splits.length);	
//				for (Split<T> split : splits) {
//					this.executor.execute(new Runnable() {
//						@Override
//						public void run() {
//							try {
//								task.execute(split.toStream(), shuffleWriter);
//							} catch (Exception e) {
//								e.printStackTrace();
//								exception.set(e);
//							} finally {
//								taskCompletionLatch.countDown();
//							}
//						}
//					});
//				}
//				try {
//					taskCompletionLatch.await();
//					if (exception.get() != null){
//						throw new IllegalStateException("Failed to execute stream", exception.get());
//					}
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//					Thread.currentThread().interrupt();
//				}
//				source = shuffleWriter.toStreamableSource();
//				finalShuffle = shuffleWriter;
//			}
//			
//			return finalShuffle.toStreamableSource().toStream();
//		} finally {
//			this.executor.shutdown();
//		}
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	private ShuffleWriterImpl createShuffleWriter(Stage stage){
		AbstractDistributable<?,?> postShuffleOperations = (AbstractDistributable<?,?>)stage.getPostShuffleOperations();
		int partitionSize = postShuffleOperations.getPartitionSize();
		Map<Integer, ConcurrentHashMap<?, ?>> partitions = new HashMap<>();
		for (int i = 0; i < partitionSize; i++) {
			partitions.put(i, new ConcurrentHashMap());
		}
//		ShuffleWriterImpl shuffleWriter = new ShuffleWriterImpl(partitions, merger.getPartitionerFunction(), merger.getMergeFunction());
//		return shuffleWriter;
		return null;
	}
}

package org.apache.dstream.tez;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.tez.io.KeyWritable;
import org.apache.dstream.tez.io.ValueWritable;
import org.apache.dstream.tez.utils.HdfsSerializerUtils;
import org.apache.dstream.tez.utils.StreamUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezTaskProcessor extends SimpleMRProcessor {
	
	private final Logger logger = LoggerFactory.getLogger(TezTaskProcessor.class);
	
	private final String dagName;
	
	private final int taskIndex;
	
	private final String vertexName;
	
	private final Configuration configuration;
	
	/**
	 * 
	 * @param context
	 */
	public TezTaskProcessor(ProcessorContext context) {
		super(context);
		this.dagName = this.getContext().getDAGName();
		this.taskIndex = this.getContext().getTaskIndex();
		this.vertexName = this.getContext().getTaskVertexName();
		this.configuration = ReflectionUtils.getFieldValue(this.getContext(), "conf", Configuration.class);
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void run() throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing processor for task: " + this.taskIndex + "; DAG " 
					+ this.dagName + "; Vertex " + this.vertexName);
		}
		
		if (this.isSingleSource()){
			Reader reader = this.getReader();
			
			Stream stream = (reader instanceof KeyValueReader) 
					? StreamUtils.toStream((KeyValueReader) reader) 
					: StreamUtils.toStream((KeyValuesReader) reader);
			
			KeyValueWriter kvWriter = (KeyValueWriter) this.getOutputs().values().iterator().next().getWriter();
			Function<Stream, Stream<Entry<Object,Object>>> streamProcessingFunction = this.extractTaskFunction();
			
			WritingConsumer<Object, Object> consume = new WritingConsumer<>(kvWriter);
			streamProcessingFunction.apply(stream).forEach(consume);
//			streamProcessingFunction.apply(stream).forEach(System.out::print);
		}

		logger.info("Finished processing task-[" + this.dagName + ":" + this.vertexName + ":" + this.taskIndex + "]");
	}
	
	
	
	private boolean isSingleSource(){
		return this.getInputs().size() == 1;
	}
	
	private Reader getReader() {
		try {
			return this.getInputs().entrySet().iterator().next().getValue().getReader();
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to get Reader", e);
		}
	}

	
	@SuppressWarnings("rawtypes")
	private Function extractTaskFunction() throws Exception {
		ObjectRegistry registry = this.getContext().getObjectRegistry();
		
		Function processingFunction = (Function) registry.get(this.vertexName);
		if (processingFunction == null){
			FileSystem fs = FileSystem.get(this.configuration);
			ByteBuffer payloadBuffer = this.getContext().getUserPayload().getPayload();
			byte[] payloadBytes = new byte[payloadBuffer.capacity()];
			payloadBuffer.get(payloadBytes);
			String taskPath = new String(payloadBytes);
			processingFunction = HdfsSerializerUtils.deserialize(new Path(taskPath), fs, Function.class);
			registry.cacheForDAG(this.vertexName, processingFunction);
//			TezDelegatingPartitioner.setSparkPartitioner(task.partitioner)
		}
		return processingFunction;
	}
	
	
	/**
	 * @param <K>
	 * @param <V>
	 */
	private static class WritingConsumer<K,V> implements Consumer<Entry<K,V>> {
		private final KeyWritable kw = new KeyWritable();
		private final ValueWritable<V> vw = new ValueWritable<>();
		private final KeyValueWriter kvWriter;
		/**
		 * 
		 * @param kvWriter
		 */
		public WritingConsumer(KeyValueWriter kvWriter){
			this.kvWriter = kvWriter;
		}
		/**
		 * 
		 */
		public void accept(Entry<K, V> pair) {
			this.kw.setValue(pair.getKey());
			this.vw.setValue(pair.getValue());
			try {
				this.kvWriter.write(this.kw, this.vw);
			} 
			catch (Exception e) {
				throw new IllegalStateException("Failed to write " + pair + " to KV Writer", e);
			}	
		}
	}
}

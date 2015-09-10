package org.apache.dstream.tez;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.tez.io.KeyWritable;
import org.apache.dstream.tez.io.TezDelegatingPartitioner;
import org.apache.dstream.tez.io.ValueWritable;
import org.apache.dstream.tez.utils.HdfsSerializerUtils;
import org.apache.dstream.tez.utils.StreamUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dstream.SerializableStreamAssets.SerFunction;
import dstream.support.PartitionIdHelper;
import dstream.utils.ReflectionUtils;

/**
 *
 */
public class TezTaskProcessor extends SimpleMRProcessor {

	private final Logger logger = LoggerFactory.getLogger(TezTaskProcessor.class);

	private final String dagName;

	private final int taskIndex;

	private final String vertexName;

	private final Configuration configuration;

	private final ThreadLocal<Integer> partitionIdHolder;

	/**
	 *
	 * @param context
	 */
	@SuppressWarnings("unchecked")
	public TezTaskProcessor(ProcessorContext context) {
		super(context);
		this.dagName = this.getContext().getDAGName();
		this.taskIndex = this.getContext().getTaskIndex();
		this.vertexName = this.getContext().getTaskVertexName();
		this.configuration = new Configuration();
		try {
			Field tl = ReflectionUtils.findField(PartitionIdHelper.class, "partitionIdHolder", ThreadLocal.class);
			tl.setAccessible(true);
			this.partitionIdHolder = (ThreadLocal<Integer>) tl.get(null);
			partitionIdHolder.set(this.taskIndex);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 *
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void run() throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing processor for task: " + this.taskIndex + "; DAG "
					+ this.dagName + "; Vertex " + this.vertexName);
		}

		List<LogicalInput> sortedInputs = this.getOrderedInputs();

		List<?> listOfStreams = sortedInputs.stream()
				.map(input -> {
					try {
						return input.getReader();
					} catch (Exception e) {
						throw new IllegalStateException("Failed to get reader", e);
					}
				})
				.map(reader -> {
					return reader instanceof KeyValueReader ? StreamUtils.toStream((KeyValueReader) reader)
							: StreamUtils.toStream((KeyValuesReader) reader);
				}).collect(Collectors.toList());

		Task task = this.getTask();
		Stream<?> functionArgument;
		if (listOfStreams.size() > 0){
			functionArgument = (Stream<?>) listOfStreams.get(0);
			if (listOfStreams.size() > 1){
				functionArgument = listOfStreams.stream();
			}
		}
		else if (task.getStreamProducingSourceSupplier() != null){
			int partitionId = this.getContext().getTaskIndex();
			functionArgument = task.getStreamProducingSourceSupplier().get();
		}
		else {
			throw new IllegalStateException("Unexpected condition. Can't determine input Stream for task");
		}

		SerFunction<Object, Stream<?>> streamProcessingFunction = this.extractTaskFunction();
		KeyValueWriter kvWriter = (KeyValueWriter) this.getOutputs().values().iterator().next().getWriter();
		WritingConsumer consume = new WritingConsumer(kvWriter);
		try {
			if (streamProcessingFunction == null){
				// partition
				// safe to cast to a single stream
				((Stream<?>)functionArgument).forEach(consume);
			}
			else {
				streamProcessingFunction.apply(functionArgument).forEach(consume);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Failed to process Tez task", e);
		}

		logger.info("Finished processing task-[" + this.dagName + ":" + this.vertexName + ":" + this.taskIndex + "]");
	}


	/**
	 *
	 */
	private List<LogicalInput> getOrderedInputs(){
		Map<String, LogicalInput> orderedInputMap = new TreeMap<String, LogicalInput>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				int a = Integer.parseInt(o1.split(":")[0]);
				int b = Integer.parseInt(o2.split(":")[0]);
				if (a == b){
					return 0;
				}
				else if (a > b){
					return 1;
				}
				return -1;
			}
		});

		orderedInputMap.putAll(this.inputs);
		return orderedInputMap.entrySet().stream().map(s -> s.getValue()).collect(Collectors.toList());
	}

	/**
	 *
	 */
	@SuppressWarnings("rawtypes")
	private SerFunction extractTaskFunction() throws Exception {
		Task task = this.getTask();
		return task.getFunction();
	}

	/**
	 *
	 */
	private Task getTask() throws Exception {
		ObjectRegistry registry = this.getContext().getObjectRegistry();
		Task task = (Task) registry.get(this.vertexName);
		if (task == null){
			FileSystem fs = FileSystem.get(this.configuration);
			ByteBuffer payloadBuffer = this.getContext().getUserPayload().getPayload();
			byte[] payloadBytes = new byte[payloadBuffer.capacity()];
			payloadBuffer.get(payloadBytes);
			String taskPath = new String(payloadBytes);
			task = HdfsSerializerUtils.deserialize(new Path(taskPath), fs, Task.class);
			registry.cacheForDAG(this.vertexName, task);
			TezDelegatingPartitioner.setDelegator(task.getClassifier());
		}
		return task;
	}

	/**
	 *
	 */
	private static class WritingConsumer implements Consumer<Object> {
		private final KeyWritable kw = new KeyWritable();
		private final ValueWritable<Object> vw = new ValueWritable<>();
		private final KeyValueWriter kvWriter;

		/**
		 *
		 */
		public WritingConsumer(KeyValueWriter kvWriter){
			this.kvWriter = kvWriter;
		}
		/**
		 *
		 */
		@Override
		@SuppressWarnings("rawtypes")
		public void accept(Object input) {
			try {

				if (input instanceof Entry){
					Entry inEntry = (Entry) input;
					if (inEntry.getKey() == null){
						Iterator iter = (Iterator) inEntry.getValue();
						while (iter.hasNext()){
							Object value = iter.next();
							this.vw.setValue(value);
							this.kvWriter.write(this.kw, this.vw);
						}
					}
					else {
						this.kw.setValue(((Entry<?,?>)input).getKey());
						this.vw.setValue(((Entry<?,?>)input).getValue());
						this.kvWriter.write(this.kw, this.vw);
					}
				}
				else {
					this.vw.setValue(input);
					this.kvWriter.write(this.kw, this.vw);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
				throw new IllegalStateException("Failed to write " + input + " to KV Writer", e);
			}
		}
	}
}

package org.apache.dstream;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.dstream.PipelineSpecification.Stage;
import org.apache.dstream.SerializableLambdas.BinaryOperator;
import org.apache.dstream.SerializableLambdas.Function;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Pair;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DefaultPipeline<T> implements CombinablePipeline<Object, Object> {
	private static final long serialVersionUID = -235015490231685337L;

	private final Logger logger = LoggerFactory.getLogger(DefaultPipeline.class);
	
	private final Object[] sources;
	
	private final List<Stage> stages = new ArrayList<>();
	
	private volatile int stageIdCounter;
	
	DefaultPipeline(Object[] sources) {
		this.sources = sources;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> CombinablePipeline<K, V> computeMappings(KeyValueProcessor<Entry<Object, Object>, K, V> processor) {
		if (logger.isDebugEnabled()){
			logger.debug("computeMappings(proc)");
		}
		return (CombinablePipeline<K, V>) this;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> CombinablePipeline<K, V> computeMappings(Function<? extends Stream<Entry<Object, Object>>, ? extends Stream<Entry<K, V>>> computeFunction) {
		
		if (logger.isDebugEnabled()){
			logger.debug("computeMappings(func)");
		}
		final int stageId = this.stageIdCounter++;
		this.stages.add(new Stage() {		
			private static final long serialVersionUID = 6622820834170983146L;
			@Override
			public Object getProcessingInstruction() {
				return computeFunction;
			}		
			@Override
			public String getName() {
				return "computeMappings";
			}			
			@Override
			public int getId() {
				return stageId;
			}
		});
		return (CombinablePipeline<K, V>) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K,V> CombinablePipeline<K,V> computeMappings(Function<? extends Stream<Entry<Object,Object>>, ? extends Stream<Entry<K,V>>> computeFunction,
			BinaryOperator<V> outputCombineFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("computeMappings(func, comb)");
		}
		return (CombinablePipeline<K, V>) this;
	}

	@Override
	public <R> Pipeline<R> computeValues(Function<? extends Stream<Entry<Object, Object>>, ? extends Stream<R>> computeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> Pipeline<R> computeValues(ValuesProcessor<Entry<Object, Object>, R> processor) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> Number count(Function<? extends Stream<Entry<Object, Object>>, ? extends Number> computeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> Number count(ItemCounter<Entry<Object, Object>> processor) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pipeline<Entry<Object, Object>> partition() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<Entry<Object, Object>> submit(String name) {
		if (logger.isDebugEnabled()){
			logger.debug("submit(" + name + ")");
		}

		Properties prop = PipelineConfigurationUtils.loadDelegatesConfig();
		
		String pipelineExecutionDelegateClassName = prop.getProperty(name);
		Assert.notEmpty(pipelineExecutionDelegateClassName, "Pipeline execution delegate for pipeline '" + name + "' "
				+ "is not provided in 'pipeline-delegates.cfg' (e.g., " + name + "=org.apache.dstream.LocalPipelineDelegate)");
		if (logger.isInfoEnabled()){
			logger.info("Pipeline execution delegate: " + pipelineExecutionDelegateClassName);
		}
				
		try {
			PipelineSpecification pipelineSpecification = this.build(name);

			Object pipelineInstance = ReflectionUtils.newDefaultInstance(Class.forName(pipelineExecutionDelegateClassName, true, Thread.currentThread().getContextClassLoader()));
			Method triggerMethod = ReflectionUtils.findMethod(pipelineInstance.getClass(), Stream.class, PipelineSpecification.class);	
			triggerMethod.setAccessible(true);
			
			return (Stream<Entry<Object, Object>>) triggerMethod.invoke(pipelineInstance, pipelineSpecification);
		} catch (Exception e) {
			String messageSuffix = "";
			if (e instanceof NoSuchMethodException){
				messageSuffix = "Probable cause: Your specified implementation '" + pipelineExecutionDelegateClassName + "' does not expose a method with the following signature - "
						+ "<anyModifier> java.util.stream.Stream<?> <anyName>(org.apache.dstream.PipelineSpecification pipelineSpecification)";
			}
			throw new IllegalStateException("Failed to execute pipeline '" + name + "'. " + messageSuffix, e);
		}
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public CombinablePipeline<Object, Iterable<Object>> groupByKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <VJ> CombinablePipeline<Object, Pair<Object, VJ>> join(CombinablePipeline<Object, VJ> pipeline) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	private PipelineSpecification build(final String name){
		PipelineSpecification specification = new PipelineSpecification() {		
			private static final long serialVersionUID = -4119037144503084569L;
			@Override
			public List<Stage> getStages() {
				return Collections.unmodifiableList(DefaultPipeline.this.stages);
			}
			
			@Override
			public List<Object> getSources() {
				return Collections.unmodifiableList(Arrays.asList(DefaultPipeline.this.sources));
			}
			
			@Override
			public String getName() {
				return name;
			}
			public String toString(){
				return "\n" + 
						"Name: " + name + "\n" +
						"Sources: " + this.getSources() + "\n" +
						"Stages: " + this.getStages();
			}
		};
		return specification;
	}

	@Override
	public CombinablePipeline<Object, Object> combine(BinaryOperator<Object> inputCombineFunction) {
		if (logger.isDebugEnabled()){
			logger.debug("combine(func)");
		}
		final int stageId = this.stageIdCounter++;
		this.stages.add(new Stage() {		
			private static final long serialVersionUID = 2352736167565206136L;
			@Override
			public Object getProcessingInstruction() {
				return inputCombineFunction;
			}		
			@Override
			public String getName() {
				return "combine";
			}			
			@Override
			public int getId() {
				return stageId;
			}
		});
		return this;
	}

	@Override
	public <KO, VO> CombinablePipeline<KO, VO> computeMappings(
			BinaryOperator<Object> inputCombineFunction,
			Function<? extends Stream<Entry<Object, Object>>, ? extends Stream<Entry<KO, VO>>> computeFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <KO, VO> CombinablePipeline<KO, VO> computeMappings(
			BinaryOperator<Object> inputCombineFunction,
			Function<? extends Stream<Entry<Object, Object>>, ? extends Stream<Entry<KO, VO>>> computeFunction,
			BinaryOperator<VO> outputCombineFunction) {
		// TODO Auto-generated method stub
		return null;
	}

	
}

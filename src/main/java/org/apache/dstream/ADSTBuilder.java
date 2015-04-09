package org.apache.dstream;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.PipelineSpecification.Stage;
import org.apache.dstream.SerializableHelpers.BinaryOperator;
import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.SerializableHelpers.Predicate;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.apache.dstream.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

class ADSTBuilder<T,R extends Distributable<T>> implements MethodInterceptor {
	
	private static Logger logger = LoggerFactory.getLogger(ADSTBuilder.class);
	
	private final R targetDistributable;
	
	private final Class<?> sourceItemType;
	
	private final SourceSupplier<?> sourcesSupplier;
	
	
	
	private final List<Stage> stages;
	
	private int stageIdCounter;
	
	private final boolean isPipeline;
	
	private StageFunctionAssembler stageFunctionAssembler;
	
	
	private MethodInvocation previousInvocation;

	/**
	 * 
	 * @param sourceItemType
	 * @param sourcesSupplier
	 * @param proxyType
	 */
	@SuppressWarnings("unchecked")
	private ADSTBuilder(Class<?> sourceItemType, SourceSupplier<?> sourcesSupplier, Class<? extends Distributable<?>> proxyType) {
		
		Assert.isTrue(DistributableStream.class.isAssignableFrom(proxyType) 
				|| DistributablePipeline.class.isAssignableFrom(proxyType), "Unsupported proxy type " + 
						proxyType + ". Supported types are " + DistributablePipeline.class + " & " + DistributableStream.class);
		
		ProxyFactory pf = new ProxyFactory();
		if (DistributablePipeline.class.isAssignableFrom(proxyType)){
			pf.addInterface(DistributablePipeline.class);
			this.isPipeline = true;
		} else {
			pf.addInterface(DistributableStream.class);
			this.stageFunctionAssembler = new StageFunctionAssembler();
			this.isPipeline = false;
		}
	
		pf.addAdvice(this);
		
		this.targetDistributable =  (R) pf.getProxy();
		this.sourceItemType = sourceItemType;
		this.sourcesSupplier = sourcesSupplier;
		this.stages = new ArrayList<Stage>();
		
		if (logger.isDebugEnabled()){
			logger.debug("Constructed builder proxy for " + Stream.of(pf.getProxiedInterfaces()).collect(Collectors.toList()));
		}
	}
	
	/**
	 * 
	 * @param sourceItemType
	 * @param sourceSuppliers
	 * @param proxyType
	 * @return
	 */
	static <T,R extends Distributable<T>> R getAs(Class<T> sourceItemType, SourceSupplier<?> sourcesSupplier, Class<? extends R> distributableType) {
		ADSTBuilder<T,R> builder = new ADSTBuilder<T,R>(sourceItemType, sourcesSupplier, distributableType);
		return builder.targetDistributable;
	}

	/**
	 * 
	 */
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		String operationName = invocation.getMethod().getName();
		Class<?>[] parameterTypes = invocation.getMethod().getParameterTypes();
		Object[] arguments = invocation.getArguments();
		
		if (this.isTriggerOperation(operationName)){
			String pipelineName = arguments.length == 1 ? arguments[0].toString() : UUID.randomUUID().toString();
			PipelineSpecification pipelineSpec = this.buildPipelineSpecification(pipelineName);
			if (logger.isInfoEnabled()){
				logger.info("Pipeline spec: " + pipelineSpec);
			}
			return this.trigger(pipelineSpec);
		} else {
			if (logger.isDebugEnabled()){
				List<String> argNames = Stream.of(parameterTypes).map(s -> s.getSimpleName()).collect(Collectors.toList());	
				logger.debug("Op:" + operationName 
						+ "(" + (argNames.isEmpty() ? "" : argNames.toString()) + ")");
			}
			
			if (operationName.equals("compute")){
				this.doCompute((ReflectiveMethodInvocation) invocation);
			}
			else if (operationName.equals("reduce")){
				this.doReduce((ReflectiveMethodInvocation) invocation);
			} else {
				throw new UnsupportedOperationException("Operation " + operationName + " is not supported");
			}
			this.previousInvocation = invocation;

			return this.targetDistributable;
		}
	}
	
	/**
	 * 
	 * @param invocation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void doCompute(ReflectiveMethodInvocation invocation){
		Function finalFunction = (Function) invocation.getArguments()[0];
		if (this.previousInvocation != null){
			if (this.previousInvocation.getMethod().getName().equals("reduce")){
				finalFunction = this.createKVStreapProcessingFunction(finalFunction);
			}
			else if (this.previousInvocation.getMethod().getName().equals("compute")){
				finalFunction = (Function) ((Function)this.previousInvocation.getArguments()[0]).andThen(finalFunction);
			}
			else {
				throw new UnsupportedOperationException("Transition from '" + invocation.getMethod().getName() + "' to '" + 
						this.previousInvocation.getMethod().getName() + "' is not supported");
			}
		}
		invocation.setArguments(new Object[]{finalFunction});
	}
	
	/**
	 * 
	 * @param invocation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void doReduce(ReflectiveMethodInvocation invocation){
		Object[] arguments = invocation.getArguments();
		Function finalFunction = new KeyValueExtractor((Function)arguments[0], (Function)arguments[1]);
		String stageName = invocation.getMethod().getName();
		if (this.previousInvocation != null) {
			if (this.previousInvocation.getMethod().getName().equals("compute")){
				Function mapperFunction = (Function) this.previousInvocation.getArguments()[0];
				finalFunction = (Function) mapperFunction.andThen(finalFunction);
				stageName = this.previousInvocation.getMethod().getName();
			} 
			else if (this.previousInvocation.getMethod().getName().equals("reduce")){			
				Function stageFunction = this.createKVStreapProcessingFunction(null);
				finalFunction = (Function) stageFunction.andThen(finalFunction);
			} 
			else {
				throw new UnsupportedOperationException("Transition from '" + invocation.getMethod().getName() + "' to '" + 
						this.previousInvocation.getMethod().getName() + "' is not supported");
			}
		} 
		Stage stage = this.constructStage(stageName, finalFunction);
		this.stages.add(stage);
	}
	
	private Function createKVStreapProcessingFunction(Function mapperFunction){
		KeyValuesStreamAggregator<?,?> aggregator = new KeyValuesStreamAggregator<>((BinaryOperator<?>) this.previousInvocation.getArguments()[2]);
		Function stageFunction = new KeyValuesAggregatingStreamProcessingFunction(mapperFunction, aggregator);
		return stageFunction;
	}
	
	/**
	 * 
	 * @param currentOperationName
	 * @param currentOperationFunction
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void createStage(String currentOperationName, Function<?,?> currentOperationFunction){
		Function composedFunction = currentOperationFunction;
		String stageName = currentOperationName;
		if (this.previousInvocation != null){
			KeyValuesStreamAggregator<?,?> aggregator = new KeyValuesStreamAggregator<>((BinaryOperator<?>) this.previousInvocation.getArguments()[0]);
			composedFunction = new KeyValuesAggregatingStreamProcessingFunction(currentOperationFunction, aggregator);
			stageName = this.previousInvocation.getMethod().getName();
		}
		Stage stage = this.constructStage(stageName, composedFunction);
		this.stages.add(stage);
	}
	
	/**
	 * 
	 * @param operationName
	 * @param processingInstructions
	 * @return
	 */
	private Stage constructStage(String operationName, Function<?,?> processingFunction) {	
		SourceSupplier<?> sources = this.stageIdCounter == 0 ? this.sourcesSupplier : null; 
		int stageId = this.stageIdCounter++;
		Stage stage = new Stage() {
			private static final long serialVersionUID = 365339577465067584L;

			@Override
			public SourceSupplier<?> getSourceSupplier() {
				return sources;
			}
			
			@Override
			public Class<?> getSourceItemType() {
				return ADSTBuilder.this.sourceItemType;
			}
			
			@Override
			public String getName() {
				return operationName;
			}
			
			@Override
			public int getId() {
				return stageId;
			}

			@Override
			public Function<?, ?> getProcessingFunction() {
				return processingFunction;
			}
		};
		if (logger.isDebugEnabled()){
			logger.debug("Constructed stage: " + stage);
		}
		return stage;
	}
	
	/**
	 * 
	 * @param operationName
	 * @return
	 */
	private boolean isTriggerOperation(String operationName) {
		return operationName.startsWith("execute");
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	private PipelineSpecification buildPipelineSpecification(String name){
//		this.createStage(this.previousInvocation.getMethod().getName(), null);
	
		Function finalFunction = (Function)this.previousInvocation.getArguments()[0];
		if (this.previousInvocation.getMethod().getName().equals("reduce")){
			KeyValuesStreamAggregator<?,?> aggregator = new KeyValuesStreamAggregator<>((BinaryOperator<?>) this.previousInvocation.getArguments()[2]);
			finalFunction = new KeyValuesAggregatingStreamProcessingFunction(aggregator);
		}
		Stage stage = this.constructStage(this.previousInvocation.getMethod().getName(), finalFunction);
		this.stages.add(stage);

		PipelineSpecification specification = new PipelineSpecification() {		
			private static final long serialVersionUID = -4119037144503084569L;
			
			@Override
			public List<Stage> getStages() {
				return Collections.unmodifiableList(ADSTBuilder.this.stages);
			}
			
			@Override
			public String getName() {
				return name;
			}
			public String toString(){
				return "\n" + 
						"Name: " + name + "\n" +
						"Source item type: " + getStages().get(0).getSourceItemType().getSimpleName() + "\n" + 
						"Stages: " + this.getStages();
			}
		};
		return specification;
	}
	
	/**
	 * 
	 *
	 */
	private static class StageFunctionAssembler {
		private List<Entry<String, Object>> streamOps = new ArrayList<Map.Entry<String,Object>>();
		
		private Collector collector;
		
		public void addIntrmediate(String name, Object function) {
			streamOps.add(Utils.kv(name, function));
		}
		
		public void setCollector(Collector collector) {
			this.collector = collector;
		}
		
		public Function<?, ?> buildFunction(){
			Function<Stream<?>, Stream<Entry<?,?>>> func = new Function<Stream<?>, Stream<Entry<?,?>>> () {
				@Override
				public Stream<Entry<?,?>> apply(Stream<?> streamIn) {
					Stream resultStream = streamIn;
					for (Entry<String, Object> entry : streamOps) {
						if (entry.getKey().equals("flatMap")){
							resultStream = resultStream.flatMap((Function)entry.getValue());
						} 
						else if (entry.getKey().equals("map")){
							resultStream = resultStream.map((Function)entry.getValue());
						}
						else if (entry.getKey().equals("filter")){
							resultStream = resultStream.filter((Predicate)entry.getValue());
						}
						else {
							throw new IllegalArgumentException("Unsupported: " + entry);
						}
					}
					if (collector != null){
						return ((Map)resultStream.collect(collector)).entrySet().stream();
					} else {
						return resultStream;
					}
				}
			};
			return func;
		}
	}
	
	/**
	 * 
	 * @param pipelineSpecification
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Stream<?>[] trigger(PipelineSpecification pipelineSpecification) {
		
		Properties prop = PipelineConfigurationUtils.loadDelegatesConfig();

		String pipelineExecutionDelegateClassName = prop.getProperty(pipelineSpecification.getName());
		Assert.notEmpty(pipelineExecutionDelegateClassName,
				"Pipeline execution delegate for pipeline '" + pipelineSpecification.getName() + "' "
						+ "is not provided in 'pipeline-delegates.cfg' (e.g., "
						+ pipelineSpecification.getName() + "=org.apache.dstream.LocalPipelineDelegate)");
		if (logger.isInfoEnabled()) {
			logger.info("Pipeline execution delegate: " + pipelineExecutionDelegateClassName);
		}

		try {
			Object pipelineInstance = ReflectionUtils.newDefaultInstance(Class
					.forName(pipelineExecutionDelegateClassName, true, 
							Thread.currentThread().getContextClassLoader()));
			Method triggerMethod = ReflectionUtils.findMethod(pipelineInstance.getClass(), Stream[].class, PipelineSpecification.class);
			triggerMethod.setAccessible(true);

			return (Stream<Entry<?, ?>>[]) triggerMethod.invoke(pipelineInstance, pipelineSpecification);
		} catch (Exception e) {
			String messageSuffix = "";
			if (e instanceof NoSuchMethodException) {
				messageSuffix = "Probable cause: Your specified implementation '"
						+ pipelineExecutionDelegateClassName
						+ "' does not expose a method with the following signature - "
						+ "<anyModifier> java.util.stream.Stream<?>[] <anyName>(org.apache.dstream.PipelineSpecification pipelineSpecification)";
			}
			throw new IllegalStateException("Failed to execute pipeline '"
					+ pipelineSpecification.getName() + "'. " + messageSuffix, e);
		}
	}
}

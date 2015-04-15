package org.apache.dstream;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.DistributablePipelineSpecification.Stage;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

/**
 * Builds an independent {@link DistributablePipelineSpecification} to be used by 
 * target execution environment.
 * 
 * @param <T> the type of the elements in the pipeline
 * @param <R> the type of {@link Distributable}.
 */
class DistributablePipelineSpecificationBuilder<T,R extends Distributable<T>> implements MethodInterceptor {
	
	private static Logger logger = LoggerFactory.getLogger(DistributablePipelineSpecificationBuilder.class);
	
	private final R targetDistributable;
	
	private final Class<?> sourceItemType;
	
	private final SourceSupplier<?> sourcesSupplier;
	
	private final List<Stage> stages;
	
	
	private int stageIdCounter;
	
	private ComposableStreamFunction composableStreamFunction;
	
	private ReflectiveMethodInvocation previousInvocation;

	/**
	 * 
	 * @param sourceItemType
	 * @param sourcesSupplier
	 * @param proxyType
	 */
	private DistributablePipelineSpecificationBuilder(Class<?> sourceItemType, SourceSupplier<?> sourcesSupplier, Class<? extends Distributable<?>> proxyType) {	
		Assert.isTrue(DistributableStream.class.isAssignableFrom(proxyType) 
				|| DistributablePipeline.class.isAssignableFrom(proxyType), "Unsupported proxy type " + 
						proxyType + ". Supported types are " + DistributablePipeline.class + " & " + DistributableStream.class);

		this.targetDistributable =  this.generateDistributableProxy(proxyType);
		this.sourceItemType = sourceItemType;
		this.sourcesSupplier = sourcesSupplier;
		this.stages = new ArrayList<Stage>();
	}
	
	/**
	 * Factory method to create an instance of {@link Distributable} 
	 * 
	 * @param sourceElementType the type of the elements of the stream
	 * @param sourcesSupplier the {@link SourceSupplier} for the sources of the stream
	 * @param distributableType a subclass of {@link Distributable}
	 * 
	 * @return an instance of {@link Distributable}
	 */
	static <T,R extends Distributable<T>> R getAs(Class<T> sourceElementType, SourceSupplier<?> sourcesSupplier, Class<? extends R> distributableType) {
		DistributablePipelineSpecificationBuilder<T,R> builder = new DistributablePipelineSpecificationBuilder<T,R>(sourceElementType, sourcesSupplier, distributableType);
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
		
		if ("executeAs".equals(operationName)){
			String pipelineName = arguments.length == 1 ? arguments[0].toString() : UUID.randomUUID().toString();
			DistributablePipelineSpecification pipelineSpec = this.buildPipelineSpecification(pipelineName);
			if (logger.isInfoEnabled()){
				logger.info("Pipeline spec: " + pipelineSpec);
			}
			return this.delegatePipelineSpecExecution(pipelineSpec);
		} 
		else {
			if (logger.isDebugEnabled()){
				List<String> argNames = Stream.of(parameterTypes).map(s -> s.getSimpleName()).collect(Collectors.toList());	
				logger.debug("Op:" + operationName 
						+ "(" + (argNames.isEmpty() ? "" : argNames.toString()) + ")");
			}
			
			
			if (this.targetDistributable instanceof DistributableStream){
				this.doDistributableStream((ReflectiveMethodInvocation) invocation);
			} 
			else if (this.targetDistributable instanceof DistributablePipeline){
				this.doDistributablePipeline((ReflectiveMethodInvocation) invocation);
			} 
			else {
				// should really never happen, but since we are dealing with a proxy, nice to have as fail all check
				throw new IllegalStateException("Unrecognized target Distributable: " + this.targetDistributable);
			}
			this.previousInvocation = (ReflectiveMethodInvocation) invocation;
			return this.targetDistributable;
		}
	}
	
	/**
	 * Processes invocation of operations invoked on the {@link DistributableStream}
	 * All operations other then 'reduce' will be gathered by the 'stageFunctionAssembler'
	 * and composed into a single stage Function applied on the {@link Stream}.
	 * Upon invocation of the 'reduce' operation a {@link KeyValueExtractorFunction} will be created
	 * from the supplied Key and Value mappers. Final Function will be composed from the results of 
	 * 'stageFunctionAssembler' composition and {@link KeyValueExtractorFunction};
	 * 
	 * @param invocation
	 */
	private void doDistributableStream(ReflectiveMethodInvocation invocation){
		String operationName = invocation.getMethod().getName();
		Object[] arguments = invocation.getArguments();
		BinaryOperator<?> aggregatorOp = null;
		
		if (operationName.equals("flatMap") || operationName.equals("map") || operationName.equals("filter")){
			this.composableStreamFunction.add(new DistributableStreamToStreamAdapterFunction(operationName, arguments[0]));
			invocation.setArguments(new Object[]{this.composableStreamFunction});
			
			this.doCompute(invocation);
		} 
		else if (operationName.equals("reduce")){
			this.previousInvocation.setArguments(new Object[]{this.composableStreamFunction, aggregatorOp});
			this.composableStreamFunction = new ComposableStreamFunction();
			
			this.doReduce(invocation);
		} 
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' is not supported");
		}
	}
	
	/**
	 * Processes invocation of operations invoked on {@link DistributablePipeline}
	 * 
	 * @param invocation
	 */
	private void doDistributablePipeline(ReflectiveMethodInvocation invocation){
		String operationName = invocation.getMethod().getName();
		if (operationName.equals("compute")){
			this.doCompute(invocation);
		}
		else if (operationName.equals("reduce")){
			this.doReduce(invocation);
		} 
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' is not supported");
		}
	}
	
	/**
	 * Transforms and preserves the actual invocation of 'compute' until stage creation.
	 * 
	 * The following transformation occur:
	 * 	- All subsequent 'compute' operations are composed into a single one.
	 * 	- If aggregator BinaryOperator is present it will be added as a second argument to 
	 *    transformed  invocation so it could be included in the stage later on.
	 * 
	 * @param invocation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void doCompute(ReflectiveMethodInvocation invocation){
		Function finalFunction = (Function) invocation.getArguments()[0];
		BinaryOperator aggregatorOp = null;
		if (this.previousInvocation != null){
			if (this.previousInvocation.getMethod().getName().equals("reduce")){
				aggregatorOp = (BinaryOperator) this.previousInvocation.getArguments()[2];
			}
			else if (this.previousInvocation.getMethod().getName().equals("compute")){
				aggregatorOp = (BinaryOperator) this.previousInvocation.getArguments()[1];
				
				Function sourceFunction = (Function)this.previousInvocation.getArguments()[0];
				finalFunction = ((Function)finalFunction).compose(sourceFunction);
			}
		}
		invocation.setArguments(new Object[]{finalFunction, aggregatorOp});
	}
	
	/**
	 * Creates stage by
	 * 	1. constructing KeyValueExtractorFunction from the KV mapping Functions provided as
	 *     0 and 1 arguments of the 'reduce' operation.
	 *  2. composing final Function if previous operation is 'compute' 
	 *  	- (KeyValueExtractorFunction + previous invocation Function)
	 *     otherwise final Function is KeyValueExtractorFunction.
	 *  3. determining aggregator BinaryOperation used to create stage
	 *  
	 *  NOTE: Aggregator used in stage creation will be converted to a Function
	 *  and composed with stage's final Function (if present) or used as final stage Function
	 *  itself if stage function is not present (e.g., classic MapReduce with nothing after reduce)
	 * 
	 * @param invocation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void doReduce(ReflectiveMethodInvocation invocation){
		Object[] arguments = invocation.getArguments();
		String stageName = invocation.getMethod().getName();
		
		Function stageFunction = new KeyValueExtractorFunction((Function)arguments[0], (Function)arguments[1]);
		
		BinaryOperator aggregatorOp = null;
		if (this.previousInvocation != null) {
			if (this.previousInvocation.getMethod().getName().equals("reduce")){	
				aggregatorOp = (BinaryOperator) this.previousInvocation.getArguments()[2];
				Assert.notNull(aggregatorOp, "'aggregatorOp' is null. Most definitely a BUG. Please report!");
			} 
			else {
				stageName = "compute";
				aggregatorOp = (BinaryOperator<?>) this.previousInvocation.getArguments()[1];
				
				Function computeFunction = (Function) this.previousInvocation.getArguments()[0];	
				stageFunction = stageFunction.compose(computeFunction);
			}
		} 

		this.addStage(stageName, stageFunction, aggregatorOp);
	}
	
	/**
	 * 
	 * @param operationName
	 * @param processingInstructions
	 * @return
	 */
	private void addStage(String operationName, Function<Stream<?>, Stream<?>> processingFunction, BinaryOperator<?> aggregatorOp) {	
		SourceSupplier<?> sources = this.stageIdCounter == 0 ? this.sourcesSupplier : null; 
		int stageId = this.stageIdCounter++;
		Stage stage = new Stage() {
			private static final long serialVersionUID = 365339577465067584L;
			
			@Override
			public SourceSupplier<?> getSourceSupplier() {
				return sources;
			}
			
			@Override
			public BinaryOperator<?> getAggregatorOperator() {
				return aggregatorOp;
			}
			
			@Override
			public Class<?> getSourceItemType() {
				return DistributablePipelineSpecificationBuilder.this.sourceItemType;
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
			public Function<Stream<?>, Stream<?>> getProcessingFunction() {
				return processingFunction;
			}
		};
		if (logger.isDebugEnabled()){
			logger.debug("Constructed stage: " + stage);
		}
		this.stages.add(stage);
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	private DistributablePipelineSpecification buildPipelineSpecification(String name){

		this.createLastStage();

		DistributablePipelineSpecification specification = new DistributablePipelineSpecification() {		
			private static final long serialVersionUID = -4119037144503084569L;
			
			@Override
			public List<Stage> getStages() {
				return Collections.unmodifiableList(DistributablePipelineSpecificationBuilder.this.stages);
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
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void createLastStage(){
		if (this.previousInvocation.getMethod().getName().equals("reduce")){
			this.addStage(this.previousInvocation.getMethod().getName(), null, (BinaryOperator)this.previousInvocation.getArguments()[2]);
		} 
		else {
			Object[] args = this.previousInvocation.getArguments();
			Function stageFunction = (Function) args[0];
			BinaryOperator aggregatorOp = args.length == 2 ? (BinaryOperator<?>)args[1] : null;
			this.addStage(this.previousInvocation.getMethod().getName(), stageFunction, aggregatorOp);
		}
	}
	
	/**
	 * 
	 * @param pipelineSpecification
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Stream<Stream<?>> delegatePipelineSpecExecution(DistributablePipelineSpecification pipelineSpecification) {
		
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
			ExecutionDelegate pipelineExecutionDelegate = (ExecutionDelegate) ReflectionUtils.newDefaultInstance(Class
					.forName(pipelineExecutionDelegateClassName, true, 
							Thread.currentThread().getContextClassLoader()));
			Method delegateMethod = ReflectionUtils.findMethod(pipelineExecutionDelegate.getClass(), Stream[].class, DistributablePipelineSpecification.class);
			delegateMethod.setAccessible(true);

			Stream<?>[] resultStreams =  (Stream<?>[]) delegateMethod.invoke(pipelineExecutionDelegate, pipelineSpecification);
			return (Stream<Stream<?>>) this.generateResultProxy(Stream.of(resultStreams), pipelineExecutionDelegate.getCloseHandler());
		} 
		catch (Exception e) {
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
	
	/**
	 * Creates proxy over the result Stream to ensures that close() call is always delegated to
	 * the close handler provided by the target ExecutionDelegate.
	 * 
	 * @param resultStream
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Stream<?> generateResultProxy(Stream<?> resultStream, Runnable closeHandler){
		resultStream.onClose(closeHandler);
		ProxyFactory pf = new ProxyFactory(resultStream);
		pf.addAdvice(new MethodInterceptor() {	
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				Object result = invocation.proceed();
				if (Stream.class.isAssignableFrom(invocation.getMethod().getReturnType())){
					Stream<?> stream = (Stream<?>) result;
					result = generateResultProxy(stream, closeHandler);
				}
				return result;
			}
		});
		return (Stream<Stream<?>>) pf.getProxy();
	}
	
	/**
	 * 
	 * @param proxyType
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private R generateDistributableProxy(Class<?> proxyType){
		ProxyFactory pf = new ProxyFactory();
		if (DistributablePipeline.class.isAssignableFrom(proxyType)){
			pf.addInterface(DistributablePipeline.class);
		} 
		else {
			pf.addInterface(DistributableStream.class);
			this.composableStreamFunction = new ComposableStreamFunction();
		}
	
		pf.addAdvice(this);
		if (logger.isDebugEnabled()){
			logger.debug("Constructed builder proxy for " + Stream.of(pf.getProxiedInterfaces()).collect(Collectors.toList()));
		}
		return (R) pf.getProxy();
	}
}
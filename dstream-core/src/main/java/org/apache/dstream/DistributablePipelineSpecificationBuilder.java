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
import org.apache.dstream.utils.Pair;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

/**
 * Builds specification of data processing pipeline which could be executed in 
 * the distributed environment.
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
		
		if ("executeAs".equals(operationName)){
			String pipelineName = this.determinePipelineName(invocation);
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
				// should really never happen, but since we are dealing with a proxy, nice to have as fail-all check
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
	 * Once composed, stage function is treated as just another pipeline operation hence 
	 * the delegation to the doDistributablePipeline() method.
	 */
	private void doDistributableStream(ReflectiveMethodInvocation invocation){
		String operationName = invocation.getMethod().getName();		
		if (this.isInStageOperation(operationName)){
			this.composableStreamFunction.add(new DistributableStreamToStreamAdapterFunction(operationName, invocation.getArguments()[0]));
			this.updateInvocationArguments(invocation, this.composableStreamFunction, null, null);
		} 
		else if (this.isStageBoundaryOperation(operationName)){
			this.updateInvocationArguments(this.previousInvocation, this.composableStreamFunction, null, null);
			this.composableStreamFunction = new ComposableStreamFunction();
		} 
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' is not supported");
		}
		
		this.doDistributablePipeline(invocation);
	}
	
	/**
	 * Processes invocation of operations invoked on {@link DistributablePipeline}
	 */
	private void doDistributablePipeline(ReflectiveMethodInvocation invocation){
		String operationName = invocation.getMethod().getName();
		if (this.isInStageOperation(operationName)){
			this.processInStageInvocation(invocation);
		}
		else if (this.isStageBoundaryOperation(operationName)){
			this.processStageBoundaryInvocation(invocation);
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
	 */
	@SuppressWarnings("rawtypes")
	private void processInStageInvocation(ReflectiveMethodInvocation invocation){
		Function finalFunction = (Function) invocation.getArguments()[0];	
		Pair<Function, BinaryOperator> stageOperations = this.gatherStageOperations(finalFunction);
		
		this.updateInvocationArguments(invocation, stageOperations._1(), null, stageOperations._2());
	}
	
	/**
	 * Creates stage by
	 * 	1. constructing KeyValueExtractorFunction from the KV mapping Functions provided as
	 *     0 and 1 arguments of the 'reduce' operation.
	 *  2. composing final Function if previous operation is an in-stage operation
	 *  	- (previous Function + KeyValueExtractorFunction)
	 *     otherwise final Function is KeyValueExtractorFunction.
	 *  3. determining the stage's aggregator
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void processStageBoundaryInvocation(ReflectiveMethodInvocation invocation){
		Object[] arguments = invocation.getArguments();
		Function stageFunction = new KeyValueExtractorFunction((Function)arguments[0], (Function)arguments[1]);	
		Pair<Function, BinaryOperator> stageOperations = this.gatherStageOperations(stageFunction);
		
		this.addStage(stageOperations._1(), stageOperations._2());
	}
	
	/**
	 * Will compose provided 'stageFunction' with function from the previous invocation 
	 * if such invocation exists and represents an intermediate invocation (e.g., 'map', 
	 * 'flatMap' etc., and not 'reduce'). Otherwise the provided stageFunction remains unchanged.
	 * 
	 * Will extract aggregator (BinaryOperator) from the previous invocation.
	 * 
	 * Will gather final function and aggregator into a pair
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Pair<Function, BinaryOperator> gatherStageOperations(Function stageFunction){
		BinaryOperator aggregatorOp = null;
		Function finalFunction = stageFunction;
		if (this.previousInvocation != null) {	
			aggregatorOp = (BinaryOperator) this.previousInvocation.getArguments()[2];
			if (this.isInStageOperation(this.previousInvocation.getMethod().getName())){
				Function sourceFunction = (Function)this.previousInvocation.getArguments()[0];
				finalFunction = stageFunction.compose(sourceFunction);
			}
		} 
		return new Pair<Function, BinaryOperator>(finalFunction, aggregatorOp);
	}
	
	/**
	 * 
	 */
	private void addStage(Function<Stream<?>, Stream<?>> processingFunction, BinaryOperator<?> aggregatorOp) {	
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
				return "STAGE_" + this.getId();
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
	 * By this point previousInvocation is either current invocation or contains a 
	 * composition (if using Streams API style). 
	 * However we are only extracting stage function from it if previous invocation 
	 * is an in-stage invocation (e.g., map, flatMap, compute etc.), otherwise it is known
	 * that previous stage was already created (see processStageBoundaryInvocation() method) 
	 * leaving this last stage to only contain the aggregator.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void createLastStage(){
		Function<Stream<?>, Stream<?>> stageFunction = null;
		BinaryOperator<?> aggregatorOp = (BinaryOperator)this.previousInvocation.getArguments()[2];
		if (this.isInStageOperation(this.previousInvocation.getMethod().getName())){
			stageFunction = (Function) this.previousInvocation.getArguments()[0];
		}
		this.addStage(stageFunction, aggregatorOp);
	}
	
	/**
	 * 
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
	
	/**
	 * 
	 */
	private String determinePipelineName(MethodInvocation invocation){
		String pipelineName;
		if (invocation.getArguments().length == 1){
			pipelineName = invocation.getArguments()[0].toString();
		}
		else {
			pipelineName = UUID.randomUUID().toString();
			if (logger.isInfoEnabled()){
				logger.info("Generated pipeline name as: ");
			}
		}
		return pipelineName;
	}
	
	/**
	 * 
	 */
	private boolean isInStageOperation(String operationName){
		return operationName.equals("flatMap") || 
			   operationName.equals("map") || 
			   operationName.equals("filter") ||
			   operationName.equals("compute");
	}
	
	/**
	 * 
	 */
	private boolean isStageBoundaryOperation(String operationName){
		return operationName.equals("reduce");
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private void updateInvocationArguments(ReflectiveMethodInvocation invocation, Function f1, Function f2, BinaryOperator aggregatorOp) {
		invocation.setArguments(new Object[]{f1, f2, aggregatorOp});
	}
}
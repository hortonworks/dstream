package org.apache.dstream;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.ExecutionContextSpecification.Stage;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceFilter;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.framework.ReflectiveMethodInvocation;

/**
 * Builds data processing pipeline specification by capturing and interpreting 
 * operations on {@link DistributablePipeline} and {@link DistributableStream}.
 * 
 * @param <T> the type of the elements in the pipeline
 * @param <R> the type of {@link DistributableExecutable}.
 */
class ExecutionContextSpecificationBuilder<T,R extends DistributableExecutable<T>> implements MethodInterceptor {
	
	private static Logger logger = LoggerFactory.getLogger(ExecutionContextSpecificationBuilder.class);
	
	private final R targetDistributable;
	
	private final Class<?> sourceItemType;
	
	private final String pipelineName;
	
	
	private int stageIdCounter;
	
	private ComposableStreamFunction composableStreamFunction;
	
	private ReflectiveMethodInvocation previousInvocation;

	/**
	 * 
	 * @param sourceItemType
	 * @param sourcesSupplier
	 * @param proxyType
	 */
	private ExecutionContextSpecificationBuilder(Class<?> sourceItemType, String pipelineName, Class<? extends DistributableExecutable<?>> proxyType) {	
		Assert.isTrue(DistributableStream.class.isAssignableFrom(proxyType) 
				|| DistributablePipeline.class.isAssignableFrom(proxyType), "Unsupported proxy type " + 
						proxyType + ". Supported types are " + DistributablePipeline.class + " & " + DistributableStream.class);

		this.targetDistributable =  this.generateDistributableProxy(proxyType);
		this.sourceItemType = sourceItemType;
		this.pipelineName = pipelineName;
	}
	
	/**
	 * Factory method to create an instance of {@link DistributableExecutable} 
	 * 
	 * @param sourceElementType the type of the elements of the stream
	 * @param sourcesSupplier the {@link SourceSupplier} for the sources of the stream
	 * @param distributableType a subclass of {@link DistributableExecutable}
	 * 
	 * @return an instance of {@link DistributableExecutable}
	 */
	static <T,R extends DistributableExecutable<T>> R getAs(Class<T> sourceElementType, String pipelineName, Class<? extends R> distributableType) {
		ExecutionContextSpecificationBuilder<T,R> builder = new ExecutionContextSpecificationBuilder<T,R>(sourceElementType, pipelineName, distributableType);
		return builder.targetDistributable;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		String operationName = invocation.getMethod().getName();
		Class<?>[] parameterTypes = invocation.getMethod().getParameterTypes();
		Object[] arguments = invocation.getArguments();
		Object returnValue;
		if ("executeAs".equals(operationName)){
			String executionName = arguments[0].toString();
			Assert.notEmpty(executionName, "'executionName' must not be null or empty");
			
			List<Stage> stages = (List<Stage>)this.targetDistributable;
			/*
			 * This will create a default stage with wysiwyg function
			 * for condition where executeAs was invoked on the pipeline that had no other invocations
			 * essentially returning source data
			 */
			if (stages.size() == 0){
				this.addStage(wysiwyg -> wysiwyg, null);
			}
			
			Properties executionProperties = PipelineConfigurationUtils.loadExecutionConfig(executionName);
			String output = executionProperties.getProperty(DistributableConstants.OUTPUT);
			if (output != null){
				Assert.isTrue(SourceSupplier.isURI(output), "URI '" + output + "' must have scheme defined (e.g., file:" + output + ")");
			}
			
			ExecutionContextSpecification executionContextSpec = 
					this.buildExecutionContextSpecification(executionName, output == null ? null : new URI(output));
			if (logger.isInfoEnabled()){
				logger.info("Execution context spec: " + executionContextSpec);
			}
			
			String sourceProperty = executionProperties.getProperty(DistributableConstants.SOURCE + "." + this.pipelineName);
			Assert.notEmpty(sourceProperty, "'source." + this.pipelineName +  "' property can not be found in " + 
							executionContextSpec.getName() + ".cfg configuration file.");
			
			SourceSupplier sourceSupplier = SourceSupplier.create(sourceProperty, arguments.length == 2 ? (SourceFilter<?>)arguments[1] : null);

			stages.get(0).setSourceSupplier(sourceSupplier);
			
			returnValue = this.delegatePipelineSpecExecution(executionContextSpec);
		} 
		else if (this.isStageBoundaryOperation(operationName) || this.isStageOperation(operationName)) {
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
			
			returnValue = this.targetDistributable;
		}
		else {
			returnValue = invocation.proceed();
		}
		return returnValue;
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
		if (this.isStageOperation(operationName)){
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
		if (this.isStageOperation(operationName)){
			this.processInStageInvocation(invocation);
		}
		else if (this.isStageBoundaryOperation(operationName)){
			this.processStageBoundaryInvocation(invocation);
		} 
		else if (operationName.equals("join")){
			System.out.println("Join");
//			invocation.getArguments()[0]
		}
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' is not supported");
		}
	}
	
	/**
	 * Holds the actual invocation of 'compute' until subsequent invocation of stage boundary operation.
	 * In the event where the subsequent invocation is another 'compute' the two will be composed 
	 * into one.
	 * 
	 * In the event where stage boundary operation is followed by a another 'compute', 
	 * (e.g., compute->reduce->compute - a two stage DAG) the aggregator provided by the stage boundary 
	 * operation will become part of the subsequent 'compute' stage.
	 */
	@SuppressWarnings("unchecked")
	private void processInStageInvocation(ReflectiveMethodInvocation invocation){
		Function<Stream<?>, Stream<?>> stageFunction = (Function<Stream<?>, Stream<?>>) invocation.getArguments()[0];	

		if (((List<Stage>)this.targetDistributable).size() == 0){
			this.addStage(stageFunction, null);
		}
		else {
			this.composeWithLastStageFunction(stageFunction);
		}
	}
	
	/**
	 * 1. Constructs KeyValueExtractorFunction from the KV mapping Functions provided as
	 *     0 and 1 arguments of the 'reduce' operation.
	 * 2. Composes KeyValueExtractorFunction into the last stage function
	 * 3. Creates a new Stage with null function and aggregator. In cases where 
	 *    there are subsequent 'compute' operations, their functions will be merged and 
	 *    injected into this new Stage.
	 */
	@SuppressWarnings("unchecked")
	private void processStageBoundaryInvocation(ReflectiveMethodInvocation invocation){
		Object[] arguments = invocation.getArguments();
		Function<Stream<?>, Stream<?>> kvExtractorFunction = 
				new KeyValueExtractorFunction((Function<?,?>)arguments[0], (Function<?,?>)arguments[1]);	

		if (((List<Stage>)this.targetDistributable).size() > 0){
			this.composeWithLastStageFunction(kvExtractorFunction);
		}
		else {
			this.addStage(kvExtractorFunction, null);
		}
		this.addStage(null, (BinaryOperator<Object>)arguments[2]);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void composeWithLastStageFunction(Function<Stream<?>, Stream<?>> composeFunction){
		List<Stage> stages = (List<Stage>)this.targetDistributable;
		Stage stage = stages.get(stages.size()-1);
		if (!(composeFunction instanceof ComposableStreamFunction)){
			Function<Stream<?>, Stream<?>> newFunction = composeFunction;
			Function<Stream<?>, Stream<?>> currentFunction = stage.getProcessingFunction();
			if (currentFunction != null){
				newFunction = composeFunction.compose(currentFunction);
			}
			stage.setProcessingFunction(newFunction);
		}
		else if (stage.getProcessingFunction() == null){
			stage.setProcessingFunction(composeFunction);
		}
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void addStage(Function<Stream<?>, Stream<?>> processingFunction, BinaryOperator<Object> aggregatorOp) {	
		int stageId = this.stageIdCounter++;
		Stage stage = new Stage() {
			private static final long serialVersionUID = 365339577465067584L;
			
			@Override
			public BinaryOperator<Object> getAggregatorOperator() {
				return aggregatorOp;
			}
			
			@Override
			public Class<?> getSourceItemType() {
				return ExecutionContextSpecificationBuilder.this.sourceItemType;
			}
			
			@Override
			public String getName() {
				return pipelineName + "$STAGE_" + this.getId();
			}
			
			@Override
			public int getId() {
				return stageId;
			}
		};
		if (processingFunction != null){
			stage.setProcessingFunction(processingFunction);
		}
		
		if (logger.isDebugEnabled()){
			logger.debug("Constructed stage: " + stage);
		}
		
		((List<Stage>)this.targetDistributable).add(stage);
	}
	
	/**
	 * 
	 */
	private ExecutionContextSpecification buildExecutionContextSpecification(String name, URI outputPath){
		ExecutionContextSpecification specification = new ExecutionContextSpecification() {		
			private static final long serialVersionUID = -4119037144503084569L;
			
			@SuppressWarnings("unchecked")
			@Override
			public List<Stage> getStages() {
				return Collections.unmodifiableList((List<Stage>) ExecutionContextSpecificationBuilder.this.targetDistributable);
			}
			
			@Override
			public String getName() {
				return name;
			}
			public String toString() {
				List<Stage> stages = this.getStages();
				return "\n" + 
						"Name: " + name + "\n" +
						"Output path: " + outputPath +"\n" +
						"Source item type: " + (stages.size() > 0 ? getStages().get(0).getSourceItemType().getSimpleName() : "<>")  + "\n" + 
						"Stages: " + this.getStages();
			}

			@Override
			public URI getOutputUri() {
				return outputPath;
			}
		};
		return specification;
	}
	
	/**
	 * 
	 */
	private Future<Stream<Stream<?>>> delegatePipelineSpecExecution(ExecutionContextSpecification pipelineSpecification) {	
		Properties prop = PipelineConfigurationUtils.loadDelegatesConfig();

		String pipelineExecutionDelegateClassName = prop.getProperty(pipelineSpecification.getName());
		Assert.notEmpty(pipelineExecutionDelegateClassName,
				"Pipeline execution delegate for pipeline '" + pipelineSpecification.getName() + "' "
						+ "is not provided in 'pipeline-delegates.cfg' (e.g., "
						+ pipelineSpecification.getName() + "=foo.bar.SomePipelineDelegate)");
		if (logger.isInfoEnabled()) {
			logger.info("Pipeline execution delegate: " + pipelineExecutionDelegateClassName);
		}

		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			ExecutionDelegate pipelineExecutionDelegate = (ExecutionDelegate) ReflectionUtils.newDefaultInstance(Class
					.forName(pipelineExecutionDelegateClassName, true, 
							Thread.currentThread().getContextClassLoader()));
			
			Future<Stream<Stream<?>>> resultFuture = executor.submit(new Callable<Stream<Stream<?>>>() {
				@SuppressWarnings("unchecked")
				@Override
				public Stream<Stream<?>> call() throws Exception {
					Stream<?>[] resultStreams =  pipelineExecutionDelegate.execute(pipelineSpecification);
					return (Stream<Stream<?>>) generateResultProxy(Stream.of(resultStreams), new Runnable() {
						@Override
						public void run() {
							try {
								pipelineExecutionDelegate.getCloseHandler().run();
							} 
							catch (Exception e) {
								logger.error("Failed during execution of close handler", e);
							}
							finally {
								logger.debug("Shutting down executor");
								executor.shutdownNow();
							}
						}
					});
				}
			});
			
			return this.generateResultProxyFuture(resultFuture, pipelineSpecification);
		} 
		catch (Exception e) {
			executor.shutdownNow();
			String messageSuffix = "";
			if (e instanceof ClassNotFoundException) {
				messageSuffix = "Probable cause: Your specified implementation of ExecutionDelegate '"
						+ pipelineExecutionDelegateClassName + "' can not be found.";
			}
			throw new IllegalStateException("Failed to execute pipeline '"
					+ pipelineSpecification.getName() + "'. " + messageSuffix, e);
		}
	}
	
	/**
	 * Will generate proxy over the result future which contains DistributablePipelineSpecificationExtractor 
	 * interface to allow access to DistributablePipelineSpecification
	 */
	@SuppressWarnings("unchecked")
	private Future<Stream<Stream<?>>> generateResultProxyFuture(Future<Stream<Stream<?>>> resultFuture, ExecutionContextSpecification pipelineSpecification){
		ProxyFactory pf = new ProxyFactory(resultFuture);
		pf.addInterface(ExecutionContextSpecificationExtractor.class);
		pf.addAdvice(new MethodInterceptor() {
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				if (invocation.getMethod().getName().equals("getSpecification")){
					return pipelineSpecification;
				}
				else {
					return invocation.proceed();
				}
			}
		});
		return (Future<Stream<Stream<?>>>) pf.getProxy();
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
		ProxyFactory pf = new ProxyFactory(new ArrayList<Stage>());
		
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
	private boolean isStageOperation(String operationName){
		return this.isStreamStageOperation(operationName) ||
			   operationName.equals("compute");
	}
	
	/**
	 * 
	 * @param operationName
	 * @return
	 */
	private boolean isStreamStageOperation(String operationName){
		return operationName.equals("flatMap") || 
			   operationName.equals("map") || 
			   operationName.equals("filter");
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
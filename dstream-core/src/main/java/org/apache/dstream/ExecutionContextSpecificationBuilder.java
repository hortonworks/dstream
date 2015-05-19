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
import org.apache.dstream.support.PipelineConfigurationHelper;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceFilter;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;
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
final class ExecutionContextSpecificationBuilder<T,R extends DistributableExecutable<T>> implements MethodInterceptor {
	
	private static Logger logger = LoggerFactory.getLogger(ExecutionContextSpecificationBuilder.class);
	
	private final R targetDistributable;
	
	private final Class<?> sourceItemType;
	
	private final String pipelineName;
	
	
	private int stageIdCounter;
	
	private Properties executionProperties;

	/**
	 * 
	 * @param sourceItemType
	 * @param pipelineName
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
		Object[] arguments = invocation.getArguments();
		Object returnValue;
		if ("executeAs".equals(operationName)){
			String executionName = arguments[0].toString();
			Assert.notEmpty(executionName, "'executionName' must not be null or empty");
			
			List<Stage> stages = (List<Stage>)this.targetDistributable;
			/*
			 * This will create a default stage with wysiwyg function
			 * for condition where executeAs(..) was invoked on the pipeline that had no other invocations
			 * essentially returning source data
			 */
			if (stages.size() == 0){
				this.addStage((Stream<?> wysiwyg) -> wysiwyg, null);
			}

			this.executionProperties = PipelineConfigurationHelper.loadExecutionConfig(executionName);
			String output = executionProperties.getProperty(DistributableConstants.OUTPUT);
			if (output != null){
				Assert.isTrue(SourceSupplier.isURI(output), "URI '" + output + "' must have scheme defined (e.g., file:" + output + ")");
			}
			
			ExecutionContextSpecification executionContextSpec = this.buildExecutionContextSpec(executionName, output == null ? null : new URI(output), 
							ExecutionContextSpecificationBuilder.this.targetDistributable);
			
			String sourceProperty = executionProperties.getProperty(DistributableConstants.SOURCE + "." + this.pipelineName);
			Assert.notEmpty(sourceProperty, "'source." + this.pipelineName +  "' property can not be found in " + 
							executionContextSpec.getName() + ".cfg configuration file.");
			
			SourceSupplier sourceSupplier = SourceSupplier.create(sourceProperty, arguments.length == 2 ? (SourceFilter<?>)arguments[1] : null);
			this.getInitialStage().setSourceSupplier(sourceSupplier);			
			returnValue = this.delegatePipelineSpecExecution(executionContextSpec);
			
			for (Stage stage : stages) {
				if (stage.getDependentExecutionContextSpec() != null){
					ExecutionContextSpecification dependentStageSpec = stage.getDependentExecutionContextSpec();
					Stage initialStage = dependentStageSpec.getStages().get(0);
					String depSourceProperty = executionProperties.getProperty(DistributableConstants.SOURCE + "." + dependentStageSpec.getName());
					//TODO see #37 Move SourceFilter to pipeline/stream factory method.
					SourceSupplier depStageSourceSupplier = SourceSupplier.create(depSourceProperty, null);
					initialStage.setSourceSupplier(depStageSourceSupplier);
					break;
				}
			}
			if (logger.isInfoEnabled()){
				logger.info("Execution context spec: " + executionContextSpec);
			}
		} 
		else if (this.isStageOrBoundaryOperation(operationName)) {
			if (logger.isDebugEnabled()){
				List<String> argNames = Stream.of(invocation.getMethod().getParameterTypes()).map(s -> s.getSimpleName()).collect(Collectors.toList());	
				logger.debug("Op:" + operationName + "(" + (argNames.isEmpty() ? "" : argNames.toString()) + ")");
			}
				
			this.doProcess((ReflectiveMethodInvocation) invocation);
			returnValue = this.targetDistributable;
		}
		else if (operationName.equals("getName")){
			returnValue = this.pipelineName;
		}
		else {
			returnValue = invocation.proceed();
		}
		return returnValue;
	}
	
	/**
	 * Processes invocation of operations invoked on {@link DistributablePipeline} 
	 * and {@link DistributableStream}
	 */
	@SuppressWarnings("unchecked")
	private void doProcess(ReflectiveMethodInvocation invocation){
		String operationName = invocation.getMethod().getName();	
		if (((List<Stage>)this.targetDistributable).size() == 0){
			this.addStage(null, null);
		}
		
		if (this.isStageOperation(operationName)){
			this.processStageInvocation(invocation);
		}
		else if (this.isStageBoundaryOperation(operationName)){
			this.processStageBoundaryInvocation(invocation);
		} 
		else {
			throw new UnsupportedOperationException("Operation '" + operationName + "' is not supported");
		}
	}
	
	/**
	 * Will compose incoming function into the existing stage's function
	 */
	@SuppressWarnings("unchecked")
	private void processStageInvocation(ReflectiveMethodInvocation invocation){
		Stage stage = this.getCurrentStage();
		if (this.isStreamStageOperation(invocation.getMethod().getName())){
			if (stage.getProcessingFunction() == null){
				stage.setProcessingFunction(new DistributableStreamToStreamAdapterFunction(invocation.getMethod().getName(), invocation.getArguments()[0]));
			}
			else {
				this.composeWithLastStageFunction(new DistributableStreamToStreamAdapterFunction(invocation.getMethod().getName(), invocation.getArguments()[0]));
			}
		}
		else {
			this.composeWithLastStageFunction((Function<Stream<?>, Stream<?>>) invocation.getArguments()[0]);
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void processStageBoundaryInvocation(ReflectiveMethodInvocation invocation){
		Object[] arguments = invocation.getArguments();
		
		if (invocation.getMethod().getName().equals("join")) {
			Assert.notEmpty(arguments, "All arguments of a join operation are required and can not be null");
			
			DistributableExecutable<?> dependentDistributable = (DistributableExecutable<?>) arguments[0];
			ExecutionContextSpecification dependentExecutionContextSpec = 
					this.buildExecutionContextSpec(dependentDistributable.getName(), null, dependentDistributable);
				
			Function pjFunc = new PredicateJoinFunction(
					new KeyValueMappingFunction((Function<?,?>)arguments[1], (Function<?,?>)arguments[2]),
					new KeyValueMappingFunction((Function<?,?>)arguments[3], (Function<?,?>)arguments[4]));

			Stage depStage = dependentExecutionContextSpec.getStages().get(dependentExecutionContextSpec.getStages().size()-1);
			this.addStage(pjFunc, depStage.getAggregatorOperator());
			this.getCurrentStage().setDependentExecutionContextSpec(dependentExecutionContextSpec);
		}
		else {
			this.composeWithLastStageFunction(new KeyValueMappingFunction((Function<?,?>)arguments[0], (Function<?,?>)arguments[1]));
			this.addStage(null, (BinaryOperator<Object>)arguments[2]);
		}
	}
	
	/**
	 * 
	 */
	private void composeWithLastStageFunction(Function<Stream<?>, Stream<?>> composeFunction){
		Stage stage = this.getCurrentStage();
		Function<Stream<?>, Stream<?>> newFunction = composeFunction;
		Function<Stream<?>, Stream<?>> currentFunction = (Function<Stream<?>, Stream<?>>) stage.getProcessingFunction();
		if (currentFunction != null){
			newFunction = composeFunction.compose(currentFunction);
		}
		stage.setProcessingFunction(newFunction);
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
				return this.getId() + "_" + pipelineName;
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
	private ExecutionContextSpecification buildExecutionContextSpec(String executionName, URI outputPath, DistributableExecutable<?> targetExecutable){
		ExecutionContextSpecification specification = new ExecutionContextSpecification() {		
			private static final long serialVersionUID = -4119037144503084569L;
			
			@SuppressWarnings("unchecked")
			@Override
			public List<Stage> getStages() {
				return Collections.unmodifiableList((List<Stage>) targetExecutable);
			}
			
			@Override
			public String getName() {
				return executionName;
			}
			
			public String toString() {
				List<Stage> stages = this.getStages();
				return "\n" + 
						"Name: " + executionName + "\n" +
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
		Properties prop = PipelineConfigurationHelper.loadDelegatesConfig();

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
								logger.debug("Terminating down executor");
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
		return operationName.equals("reduce") ||
			   operationName.equals("join")	;
	}
	
	/**
	 * 
	 */
	private boolean isStageOrBoundaryOperation(String operationName) {
		if (this.isStageOperation(operationName) || this.isStageBoundaryOperation(operationName)) {
			if (!(this.targetDistributable instanceof DistributableStream || 
				  this.targetDistributable instanceof DistributablePipeline)) {
				// should really never happen, but since we are dealing with a
				// proxy, nice to have as fail-all check
				throw new IllegalStateException("Unsupported DistributableExecutable: "
								+ this.targetDistributable);
			}
			return true;
		}
		return false;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private Stage getCurrentStage(){
		List<Stage> stages = (List<Stage>)this.targetDistributable;
		return stages.get(stages.size()-1);
	}
	
	@SuppressWarnings("unchecked")
	private Stage getInitialStage(){
		List<Stage> stages = (List<Stage>)this.targetDistributable;
		return stages.get(0);
	}
}
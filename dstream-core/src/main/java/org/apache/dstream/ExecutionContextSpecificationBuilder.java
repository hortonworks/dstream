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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.dstream.PipelineExecutionChain.Stage;
import org.apache.dstream.support.ConfigurationGenerator;
import org.apache.dstream.support.PipelineConfigurationHelper;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
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
	
	/*
	 * Since configuration is based on the job name most of the properties are inaccessible until "executeAs"
	 * is invoked (e.g., reducers, map-side combine) when the job name is actually known, so 
	 * the callbacks will be created as Consumers and executed when "executeAs" is invoked when those
	 * config properties are known. 
	 */
	private final List<Consumer<Properties>> postConfigInitCallbacks = new ArrayList<Consumer<Properties>>();
	
	
	private int stageIdCounter;
	

	/**
	 * 
	 * @param sourceItemType
	 * @param pipelineName
	 * @param proxyType
	 */
	private ExecutionContextSpecificationBuilder(Class<?> sourceItemType, String pipelineName, Class<? extends DistributableExecutable<?>> proxyType) {	
		Assert.isTrue(DistributableStream.class.isAssignableFrom(proxyType) 
				|| DistributablePipeline.class.isAssignableFrom(proxyType)
				|| JobGroup.class.isAssignableFrom(proxyType), "Unsupported proxy type " + 
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
	 * @param groupName
	 * @param distributableType
	 * @param distributables
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <T,R extends DistributableExecutable<T>> R asGroupExecutable(String groupName, Class<? extends R> distributableType, DistributableExecutable[] distributables) {
		ExecutionContextSpecificationBuilder<T,R> builder = new ExecutionContextSpecificationBuilder<T,R>(Object.class, groupName, distributableType);
		Stream.of(distributables).forEach(((List)builder.targetDistributable)::add);	
		return builder.targetDistributable;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		String operationName = invocation.getMethod().getName();
		Object returnValue;
		if ("executeAs".equals(operationName)){
			List<PipelineExecutionChain> pipelineEecutuionChains;
			if (this.targetDistributable instanceof JobGroup){
				pipelineEecutuionChains = (List<PipelineExecutionChain>) ((List)this.targetDistributable).stream()
						.map(r -> this.finalizeExecutionChain(invocation, (R)r)).collect(Collectors.toList());
			}
			else {
				pipelineEecutuionChains = Stream.of(this.finalizeExecutionChain(invocation, this.targetDistributable)).collect(Collectors.toList());
				
			}
			returnValue = this.delegatePipelineSpecExecution(this.targetDistributable instanceof JobGroup, pipelineEecutuionChains.toArray(new PipelineExecutionChain[]{}));
		} 
		else if (this.isStageOrBoundaryOperation(operationName)) {
			if (logger.isDebugEnabled()){
				List<String> argNames = Stream.of(invocation.getMethod().getParameterTypes()).map(s -> s.getSimpleName()).collect(Collectors.toList());	
				logger.debug("Op:" + operationName + "(" + (argNames.isEmpty() ? "" : argNames.toString()) + ")");
			}
			
			// cloning, so each instance of a flow is distinctly (instance) addressable
			ExecutionContextSpecificationBuilder clonedDistributable = this.cloneTargetDistributable();
			// process on the clone
			clonedDistributable.doProcess((ReflectiveMethodInvocation)invocation);
			
			returnValue = clonedDistributable.targetDistributable;
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
	 * 
	 */
	@SuppressWarnings("unchecked")
	private PipelineExecutionChain finalizeExecutionChain(MethodInvocation invocation, R currentDistributable){
		String operationName = invocation.getMethod().getName();
		String executionName = invocation.getArguments()[0].toString();
		Assert.notEmpty(executionName, "'executionName' must not be null or empty");
		
		boolean generateConfig = false;
		if (executionName.startsWith(DistributableConstants.GENERATE_CONF)){
			executionName = executionName.split(":")[1];
			generateConfig = true;
		}
		List<Stage> stages = (List<Stage>)currentDistributable;
		/*
		 * This will create a default stage with wysiwyg function
		 * for condition where executeAs(..) was invoked on the pipeline that had no other invocations
		 * essentially returning source data
		 */
		if (stages.size() == 0){
			this.addStage((Stream<?> wysiwyg) -> wysiwyg, null, operationName);
		}
		
		if (generateConfig){
			ConfigurationGenerator confGener = new ConfigurationGenerator(currentDistributable);
			logger.info("\n\n############ GENERATING PIPELINE CONFIGURATION ############");
			logger.info("\n" + confGener.toString());
			logger.info("\n###########################################################");
		}

		Properties executionProperties = PipelineConfigurationHelper.loadExecutionConfig(executionName);

		this.postConfigInitCallbacks.forEach(s -> s.accept(executionProperties));
		System.out.println(this.pipelineName);
		PipelineExecutionChain executionChain = this.buildExecutionChain(executionName, this.getOutputUri(this.pipelineName, executionProperties), currentDistributable);
		this.setSourceSuppliers(executionProperties, stages, currentDistributable.getName(), executionName);

		if (logger.isInfoEnabled()){
			logger.info("Execution chain: " + executionChain);
		}
		
		return executionChain;
	}
	
	/**
	 * Processes invocation of operations invoked on {@link DistributablePipeline} 
	 * and {@link DistributableStream}
	 */
	@SuppressWarnings("unchecked")
	private void doProcess(ReflectiveMethodInvocation invocation){
		String operationName = invocation.getMethod().getName();	
		if (((List<Stage>)this.targetDistributable).size() == 0){
			this.addStage(null, null, null);
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
				stage.addOperationName(invocation.getMethod().getName());
				stage.setProcessingFunction(new DistributableStreamToStreamAdapterFunction(invocation.getMethod().getName(), invocation.getArguments()[0]));
			}
			else {
				this.composeWithStageFunction(this.getCurrentStage(), new DistributableStreamToStreamAdapterFunction(invocation.getMethod().getName(), invocation.getArguments()[0]),
						invocation.getMethod().getName());
			}
		}
		else {
			this.composeWithStageFunction(this.getCurrentStage(), (Function<Stream<?>, Stream<?>>) invocation.getArguments()[0],
					invocation.getMethod().getName());
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
			// the output URI here is always null since joined execution chain is essentially an inner chain of
			// another execution chain with common output.
			PipelineExecutionChain dependentExecutionContextSpec = 
					this.buildExecutionChain(dependentDistributable.getName(), null, dependentDistributable);
				
			Function pjFunc = new PredicateJoinFunction(
					new KeyValueMappingFunction((Function<?,?>)arguments[1], (Function<?,?>)arguments[2]),
					new KeyValueMappingFunction((Function<?,?>)arguments[3], (Function<?,?>)arguments[4]));

			Stage depStage = dependentExecutionContextSpec.getStages().get(dependentExecutionContextSpec.getStages().size()-1);
			this.addStage(pjFunc, depStage.getAggregatorOperator(), invocation.getMethod().getName());
			this.getCurrentStage().setDependentExecutionContextSpec(dependentExecutionContextSpec);
		}
		else {
			//dstream.stage.ms_combine.0_foo=true
			String propertyName = DistributableConstants.MAP_SIDE_COMBINE + this.getCurrentStage().getName();
			Stage currentStage = this.getCurrentStage();
			
			Consumer<Properties> msCombineCallback = new Consumer<Properties>() {
				@Override
				public void accept(Properties executionProperties) {
					boolean mapSideCombine = executionProperties.containsKey(propertyName) 
							? Boolean.parseBoolean(executionProperties.getProperty(propertyName)) : false;
							
					BinaryOperator combiner = mapSideCombine ? (BinaryOperator)arguments[2] : null;
					composeWithStageFunction(currentStage, 
							new KeyValueMappingFunction((Function)arguments[0], (Function)arguments[1], combiner), invocation.getMethod().getName());
				}
			};
			
			this.postConfigInitCallbacks.add(msCombineCallback);
					
			this.addStage(null, (BinaryOperator)arguments[2], invocation.getMethod().getName());
		}
	}
	
	/**
	 * 
	 */
	private void composeWithStageFunction(Stage stage, Function<Stream<?>, Stream<?>> composeFunction, String operationName){
		if (operationName != null){
			stage.addOperationName(operationName);
		}
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
	private void addStage(Function<Stream<?>, Stream<?>> processingFunction, BinaryOperator<Object> aggregatorOp, String operationName) {	
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
		if (operationName != null){
			stage.addOperationName(operationName);
		}
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
	private PipelineExecutionChain buildExecutionChain(String executionName, URI outputPath, DistributableExecutable<?> targetExecutable){
		PipelineExecutionChain specification = new PipelineExecutionChain() {		
			private static final long serialVersionUID = -4119037144503084569L;
			
			@SuppressWarnings("unchecked")
			@Override
			public List<Stage> getStages() {
				return Collections.unmodifiableList((List<Stage>) targetExecutable);
			}
			
			@Override
			public String getJobName() {
				return executionName;
			}
			
			@Override
			public String getPipelineName() {
				return targetExecutable.getName();
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
	private Future<Stream<?>> delegatePipelineSpecExecution(boolean group, PipelineExecutionChain... executionChains) {	
		Properties prop = PipelineConfigurationHelper.loadDelegatesConfig();
		
		String pipelineExecutionDelegateClassName = prop.getProperty(executionChains[0].getJobName());
		Assert.notEmpty(pipelineExecutionDelegateClassName,
				"Pipeline execution delegate for pipeline '" + executionChains[0].getJobName() + "' "
						+ "is not provided in 'pipeline-delegates.cfg' (e.g., "
						+ executionChains[0].getJobName() + "=foo.bar.SomePipelineDelegate)");
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
					Stream<?> resultStreams = !group ? pipelineExecutionDelegate.execute(executionChains)[0]
								: Stream.of(pipelineExecutionDelegate.execute(executionChains));
					
					return (Stream<Stream<?>>) generateResultProxy(resultStreams, new Runnable() {
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
			return this.generateResultProxyFuture(resultFuture, executionChains);
		} 
		catch (Exception e) {
			executor.shutdownNow();
			String messageSuffix = "";
			if (e instanceof ClassNotFoundException) {
				messageSuffix = "Probable cause: Your specified implementation of ExecutionDelegate '"
						+ pipelineExecutionDelegateClassName + "' can not be found.";
			}
			throw new IllegalStateException("Failed to execute pipeline '"
					+ executionChains[0].getJobName() + "'. " + messageSuffix, e);
		}
	}
	
	
	
	/**
	 * Will generate proxy over the result future which contains DistributablePipelineSpecificationExtractor 
	 * interface to allow access to DistributablePipelineSpecification
	 */
	@SuppressWarnings("unchecked")
	private Future<Stream<?>> generateResultProxyFuture(Future<Stream<Stream<?>>> resultFuture, PipelineExecutionChain... pipelineSpecification){
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
		return (Future<Stream<?>>) pf.getProxy();
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private R generateDistributableProxy(Class<?> proxyType){
		ProxyFactory pf = new ProxyFactory(new ArrayList());
		
		if (DistributablePipeline.class.isAssignableFrom(proxyType)){
			pf.addInterface(DistributablePipeline.class);
		} 
		else if (DistributableStream.class.isAssignableFrom(proxyType)){
			pf.addInterface(DistributableStream.class);
		}
		else if (JobGroup.class.isAssignableFrom(proxyType)){
			pf.addInterface(JobGroup.class);
		}
		else {
			throw new IllegalArgumentException("Unsupported proxy type: " +  proxyType);
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
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private void setSourceSuppliers(Properties executionProperties, List<Stage> stages, String pname, String ename){
		stages.forEach(stage -> {
			if (stage.getId() == 0){
				String sourceProperty = executionProperties.getProperty(DistributableConstants.SOURCE + pname);
				Assert.notEmpty(sourceProperty, DistributableConstants.SOURCE + pname +  "' property can not be found in " + 
						ename + ".cfg configuration file.");
				SourceSupplier sourceSupplier = SourceSupplier.create(sourceProperty, null);
				stage.setSourceSupplier(sourceSupplier);
			}
			if (stage.getDependentExecutionContextSpec() != null){
				PipelineExecutionChain dependentStageSpec = stage.getDependentExecutionContextSpec();
				this.setSourceSuppliers(executionProperties, dependentStageSpec.getStages(), dependentStageSpec.getJobName(), ename);
			}
		});
	}
	
	/**
	 * There may be several pipelines in a job represented as {@link DistributableExecutable}, but 
	 * there can only be *one* executable.
	 * The executable pipeline is the one on which 'executeAs' operation is invoked and the
	 * name of such pipeline is what's passed as 'executablePipelineName'.
	 */
	private URI getOutputUri(String executablePipelineName, Properties executionProperties){
		String output = executionProperties.getProperty(DistributableConstants.OUTPUT + "." + executablePipelineName);
		if (output != null){
			Assert.isTrue(SourceSupplier.isURI(output), "URI '" + output + "' must have scheme defined (e.g., file:" + output + ")");
		}
		
		URI uri = null;
		if (output != null){
			try {
				uri = new URI(output);
			} 
			catch (Exception e) {
				throw new IllegalStateException("Failed to create URI", e);
			}
		}	
		return uri;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ExecutionContextSpecificationBuilder cloneTargetDistributable(){
		ExecutionContextSpecificationBuilder clonedDistributable = new  ExecutionContextSpecificationBuilder(this.sourceItemType, this.pipelineName, 
				this.targetDistributable instanceof DistributablePipeline ? DistributablePipeline.class : DistributableStream.class);
		((List)clonedDistributable.targetDistributable).addAll(Collections.unmodifiableList(((List)this.targetDistributable)));
		clonedDistributable.stageIdCounter = this.stageIdCounter;
		clonedDistributable.postConfigInitCallbacks.addAll(Collections.unmodifiableList(this.postConfigInitCallbacks));
		return clonedDistributable;
	}
}
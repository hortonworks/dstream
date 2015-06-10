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
import org.apache.dstream.ExecutionSpec.Stage;
import org.apache.dstream.support.Aggregators;
import org.apache.dstream.support.ConfigurationGenerator;
import org.apache.dstream.support.HashParallelizer;
import org.apache.dstream.support.Parallelizer;
import org.apache.dstream.support.PipelineConfigurationHelper;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.JvmUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds data processing pipeline specification by capturing and interpreting 
 * operations on the following instances of {@link DistributableExecutable}:<br>
 * 		- {@link DistributablePipeline}<br>
 * 		- {@link DistributableStream}<br>
 * 		- {@link ExecutionGroup}.<br>
 * 
 * @param <T> the type of the elements in the pipeline
 * @param <R> the type of {@link DistributableExecutable}.
 */
final class ExecutionSpecBuilder<T,R extends DistributableExecutable<T>> implements MethodInterceptor {
	
	private static Logger logger = LoggerFactory.getLogger(ExecutionSpecBuilder.class);
	
	private final R targetDistributable;
	
	private final Class<?> sourceItemType;
	
	private final String pipelineName;
	
	/*
	 * Since configuration is based on the job name most of the properties are inaccessible until "executeAs"
	 * is invoked (e.g., reducers, map-side combine) when the job name is actually known, so 
	 * the callbacks will be created as Consumers and executed when "executeAs" is invoked when those
	 * config properties are known. 
	 */
	private final List<Consumer<Properties>> preExecCallbacks;
	
	
	private int stageIdCounter;
	

	/**
	 * 
	 * @param sourceItemType
	 * @param pipelineName
	 * @param distributableType
	 */
	private ExecutionSpecBuilder(Class<?> sourceItemType, String pipelineName, Class<? extends DistributableExecutable<?>> distributableType) {	
		Assert.isTrue(DistributableStream.class.isAssignableFrom(distributableType) 
				|| DistributablePipeline.class.isAssignableFrom(distributableType)
				|| ExecutionGroup.class.isAssignableFrom(distributableType), "Unsupported 'DistributableExecutable' type " + 
						distributableType + ". Supported types are " + DistributablePipeline.class + " & " + DistributableStream.class);

		this.targetDistributable =  this.generateDistributable(distributableType);
		this.sourceItemType = sourceItemType;
		this.pipelineName = pipelineName;
		this.preExecCallbacks = new ArrayList<Consumer<Properties>>();
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
		ExecutionSpecBuilder<T,R> builder = new ExecutionSpecBuilder<T,R>(sourceElementType, pipelineName, distributableType);
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
		ExecutionSpecBuilder<T,R> builder = new ExecutionSpecBuilder<T,R>(Object.class, groupName, distributableType);
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
			List<ExecutionSpec> executionSpecs;
			String executionName = (String) invocation.getArguments()[0];
			if (this.targetDistributable instanceof ExecutionGroup){
				executionSpecs = (List<ExecutionSpec>) ((List)this.targetDistributable).stream()
						.map(r -> this.finalizeExecutionChain(executionName, invocation, (R)r)).collect(Collectors.toList());
			}
			else {
				executionSpecs = Stream.of(this.finalizeExecutionChain(executionName, invocation, this.targetDistributable)).collect(Collectors.toList());
				
			}
			returnValue = this.delegatePipelineSpecExecution(executionName, this.targetDistributable instanceof ExecutionGroup, executionSpecs.toArray(new ExecutionSpec[]{}));
		} 
		else if (this.isStageOrBoundaryOperation(operationName)) {
			if (logger.isDebugEnabled()){
				List<String> argNames = Stream.of(invocation.getMethod().getParameterTypes()).map(s -> s.getSimpleName()).collect(Collectors.toList());	
				logger.debug("Op:" + operationName + "(" + (argNames.isEmpty() ? "" : argNames.toString()) + ")");
			}
			// cloning, so each instance of a flow is distinctly (instance) addressable
			ExecutionSpecBuilder clonedDistributable = this.cloneTargetDistributable();
			// process on the clone
			
			clonedDistributable.doProcess(invocation);
			
			returnValue = clonedDistributable.targetDistributable;
		}
		else if (operationName.equals("getName")){
			returnValue = this.pipelineName;
		}
		else if (operationName.equals("generateConfig")){
			ConfigurationGenerator confGener = new ConfigurationGenerator(this.targetDistributable);
			logger.info("\n\n############ GENERATING PIPELINE CONFIGURATION ############");
			returnValue = confGener.toString();
			logger.info("\n" + returnValue);
			logger.info("\n###########################################################");
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
	private ExecutionSpec finalizeExecutionChain(String executionName, MethodInvocation invocation, R currentDistributable){
		String operationName = invocation.getMethod().getName();
		Assert.notEmpty(executionName, "'executionName' must not be null or empty");
		
		List<Stage> stages = (List<Stage>)currentDistributable;
		/*
		 * This will create a default stage with wysiwyg function
		 * for condition where executeAs(..) was invoked on the pipeline that had no other invocations
		 * essentially returning source data
		 */
		if (stages.size() == 0){
			this.addStage(invocation, (Stream<?> wysiwyg) -> wysiwyg, null, operationName);
		}
		
		Properties executionProperties = PipelineConfigurationHelper.loadExecutionConfig(executionName);

		this.preExecCallbacks.forEach(s -> s.accept(executionProperties));
		ExecutionSpec pipelineExecutionSpecs = this.buildExecutionChain(executionName, this.getOutputUri(this.pipelineName, executionProperties), currentDistributable);
		this.setSourceSuppliers(executionProperties, stages, currentDistributable.getName(), executionName);

		if (logger.isInfoEnabled()){
			logger.info("Execution specs: " + pipelineExecutionSpecs);
		}
		
		return pipelineExecutionSpecs;
	}
	
	/**
	 * Processes invocation of operations invoked on {@link DistributablePipeline} 
	 * and {@link DistributableStream}
	 */
	@SuppressWarnings("unchecked")
	private void doProcess(MethodInvocation invocation){
		String operationName = invocation.getMethod().getName();	
		if (((List<Stage>)this.targetDistributable).size() == 0){
			this.addStage(invocation, null, null, null);
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
	private void processStageInvocation(MethodInvocation invocation){
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
	private void processStageBoundaryInvocation(MethodInvocation invocation){
		Object[] arguments = invocation.getArguments();
		
		if (invocation.getMethod().getName().equals("join")) {
			Assert.notEmpty(arguments, "All arguments of a join operation are required and can not be null");
			
			DistributableExecutable<?> dependentDistributable = (DistributableExecutable<?>) arguments[0];
			// the output URI here is always null since joined execution chain is essentially an inner chain of
			// another execution chain with common output.
			ExecutionSpec dependentPipelineEecutuionSpec = 
					this.buildExecutionChain(dependentDistributable.getName(), null, dependentDistributable);
				
			Function pjFunc = new PredicateJoinFunction(
					new KeyValueMappingFunction((Function<?,?>)arguments[1], (Function<?,?>)arguments[2]),
					new KeyValueMappingFunction((Function<?,?>)arguments[3], (Function<?,?>)arguments[4]));

			Stage depStage = dependentPipelineEecutuionSpec.getStages().get(dependentPipelineEecutuionSpec.getStages().size()-1);
			this.addStage(invocation, pjFunc, depStage.getAggregatorOperator(), invocation.getMethod().getName());
			this.getCurrentStage().setDependentExecutionSpec(dependentPipelineEecutuionSpec);
		}
		else if (invocation.getMethod().getName().equals("parallel")) {
			if (arguments.length == 1){
				if (arguments[0] instanceof Integer){
					this.getCurrentStage().setParallelizer(new HashParallelizer((Integer)arguments[0]));
				}
				else {
					this.getCurrentStage().setParallelizer((Parallelizer)arguments[0]);
				}
			}
			else {
				this.getCurrentStage().setParallelizer(new HashParallelizer((Integer)arguments[0], (Function)arguments[1]));
			}	
		}
		else if (invocation.getMethod().getName().equals("reduce")) {
			this.addMapSideCombineSettingCallback(invocation);
			this.addStage(invocation, null, (BinaryOperator)arguments[2], invocation.getMethod().getName());
		}
		else if (invocation.getMethod().getName().equals("group")) {
			this.addStage(invocation, null, Aggregators::aggregateFlatten, invocation.getMethod().getName());
		}
		else {
			throw new IllegalArgumentException("Unrecognized invocation: " + invocation.getMethod());
		}
	}
	
	/**
	 * 
	 */
	private void addMapSideCombineSettingCallback(MethodInvocation invocation){
		Object[] arguments = invocation.getArguments();
		String propertyName = DistributableConstants.MAP_SIDE_COMBINE + this.getCurrentStage().getName();
		Stage currentStage = this.getCurrentStage();
		Consumer<Properties> configCallback = new Consumer<Properties>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public void accept(Properties executionProperties) {
				boolean mapSideCombine = executionProperties.containsKey(propertyName) 
						? Boolean.parseBoolean(executionProperties.getProperty(propertyName)) : false;
						
				BinaryOperator combiner = mapSideCombine ? (BinaryOperator)arguments[2] : null;
				composeWithStageFunction(currentStage, 
						new KeyValueMappingFunction((Function)arguments[0], (Function)arguments[1], combiner), invocation.getMethod().getName());
			}
		};	
		this.preExecCallbacks.add(configCallback);
	}
	
	/**
	 * 
	 */
	private void addParallelismSettingCallback(MethodInvocation invocation){
		Stage currentStage = this.getCurrentStage();
		Consumer<Properties> configCallback = new Consumer<Properties>() {
			@SuppressWarnings({"unchecked", "rawtypes"})
			@Override
			public void accept(Properties executionProperties) {
				String parallelismConfigValue = executionProperties.containsKey(DistributableConstants.PARALLELISM + currentStage.getName()) 
						? executionProperties.getProperty(DistributableConstants.PARALLELISM + currentStage.getName())
								: null;
				// config takes precedence over code 
				if (parallelismConfigValue != null){
					String[] pParts = parallelismConfigValue.split(",");
					if (pParts.length == 1){
						if (currentStage.getParallelizer() != null){
							currentStage.getParallelizer().updatePartitionSize(Integer.parseInt(pParts[0].trim()));
						}
						else {
							currentStage.setParallelizer(new HashParallelizer(Integer.parseInt(pParts[0].trim())));
						}
					}
					else if (pParts.length == 2){
						Parallelizer parallelizer = 
								ReflectionUtils.newInstance(pParts[1].trim(), new Class[]{int.class}, new Object[]{Integer.parseInt(pParts[0].trim())});
						currentStage.setParallelizer(parallelizer);
					}
					else {
						throw new IllegalStateException("Invalid parallelization configuration: " + parallelismConfigValue);
					}
				}
				else {
					Parallelizer parallelizer = new HashParallelizer(1);
					if (invocation.getMethod().getName().equals("parallel")){
						if (invocation.getArguments().length == 1){
							if (invocation.getArguments()[0] instanceof Integer){
								parallelizer.updatePartitionSize((int) invocation.getArguments()[0]);
							}
							else {
								parallelizer = (HashParallelizer) invocation.getArguments()[0];
							}
						}
						else {
							parallelizer = new HashParallelizer((int) invocation.getArguments()[0], (Function)invocation.getArguments()[1]);
						}
					}
					else if (invocation.getMethod().getName().equals("reduce")){
						if (invocation.getArguments().length == 4){
							parallelizer = invocation.getArguments()[3] instanceof Integer 
									? new HashParallelizer((int) invocation.getArguments()[3]) 
										: (Parallelizer) invocation.getArguments()[3];
						}
					}
					else if (invocation.getMethod().getName().equals("group")){
						if (invocation.getArguments().length == 3){
							parallelizer = invocation.getArguments()[2] instanceof Integer 
									? new HashParallelizer((int) invocation.getArguments()[2]) 
										: (Parallelizer) invocation.getArguments()[2];
						}
					}
					else if (invocation.getMethod().getName().equals("join")){
						if (invocation.getArguments().length == 6){
							parallelizer = invocation.getArguments()[5] instanceof Integer 
									? new HashParallelizer((int) invocation.getArguments()[5]) 
										: (Parallelizer) invocation.getArguments()[5];
						}
					}
					currentStage.setParallelizer(parallelizer);
				}
			}
		};		
		this.preExecCallbacks.add(configCallback);
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
	private void addStage(MethodInvocation invocation, Function<Stream<?>, Stream<?>> processingFunction, BinaryOperator<Object> aggregatorOp, String operationName) {	
		int stageId = this.stageIdCounter++;
		Stage stage = new Stage() {
			@Override
			public BinaryOperator<Object> getAggregatorOperator() {
				return aggregatorOp;
			}
			
			@Override
			public Class<?> getSourceItemType() {
				return ExecutionSpecBuilder.this.sourceItemType;
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
		this.addParallelismSettingCallback(invocation);
	}
	
	/**
	 * 
	 */
	private ExecutionSpec buildExecutionChain(String executionName, URI outputPath, DistributableExecutable<?> targetExecutable){
		ExecutionSpec pipelineExecutionSpec = new ExecutionSpec() {		
			
			@SuppressWarnings("unchecked")
			@Override
			public List<Stage> getStages() {
				return Collections.unmodifiableList((List<Stage>) targetExecutable);
			}
			
			@Override
			public String getName() {
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
		return pipelineExecutionSpec;
	}
	
	/**
	 * 
	 */
	private Future<Stream<Stream<?>>> delegatePipelineSpecExecution(String executionName, boolean group, ExecutionSpec... pipelineExecutionSpecs) {	
		Properties config = PipelineConfigurationHelper.loadDelegatesConfig();
		
		String pipelineExecutionDelegateClassName = config.getProperty(executionName);
		Assert.notEmpty(pipelineExecutionDelegateClassName,
				"Pipeline execution delegate for pipeline '" + executionName + "' "
						+ "is not provided in 'pipeline-delegates.cfg' (e.g., "
						+ executionName + "=foo.bar.SomePipelineDelegate)");
		if (logger.isInfoEnabled()) {
			logger.info("Pipeline execution delegate: " + pipelineExecutionDelegateClassName);
		}

		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			ExecutionDelegate pipelineExecutionDelegate = (ExecutionDelegate) ReflectionUtils.newDefaultInstance(Class
					.forName(pipelineExecutionDelegateClassName, true, Thread.currentThread().getContextClassLoader()));
			
			Future<Stream<Stream<?>>> resultFuture = executor.submit(new Callable<Stream<Stream<?>>>() {
				@SuppressWarnings("unchecked")
				@Override
				public Stream<Stream<?>> call() throws Exception {
					Stream<?> resultStreams = group 
							? Stream.of(pipelineExecutionDelegate.execute(executionName, pipelineExecutionSpecs))
								: pipelineExecutionDelegate.execute(executionName, pipelineExecutionSpecs)[0];

					return (Stream<Stream<?>>) mixinWithCloseHandler(resultStreams, new Runnable() {
						@Override
						public void run() {
							try {
								pipelineExecutionDelegate.getCloseHandler().run();
							} 
							catch (Exception e) {
								logger.error("Failed during execution of close handler", e);
							}
							finally {
								executor.shutdownNow();
							}
						}
					});
				}
			});		
			if (config.containsKey("test")){
				return this.mixinWithExecutionSpecExtractor(resultFuture, pipelineExecutionSpecs);
			}
			else {
				return resultFuture;
			}		
		} 
		catch (Exception e) {
			executor.shutdownNow();
			String messageSuffix = "";
			if (e instanceof ClassNotFoundException) {
				messageSuffix = "Probable cause: Your specified implementation of ExecutionDelegate '"
						+ pipelineExecutionDelegateClassName + "' can not be found.";
			}
			throw new IllegalStateException("Failed to execute pipeline '"
					+ executionName + "'. " + messageSuffix, e);
		}
	}
	
	
	
	/**
	 * Will generate proxy over the result future which contains {@link ExecutionSpecExtractor} 
	 * interface to allow access to and array of {@link ExecutionSpec}s
	 */
	private Future<Stream<Stream<?>>> mixinWithExecutionSpecExtractor(Future<Stream<Stream<?>>> resultFuture, ExecutionSpec... pipelineExecutionSpecs){
		MethodInterceptor advice = new MethodInterceptor() {
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				if (invocation.getMethod().getName().equals("getExecutionSpec")){
					return pipelineExecutionSpecs;
				}
				return invocation.proceed();
			}
		};
		return JvmUtils.proxy(resultFuture, advice, ExecutionSpecExtractor.class);
	}
	
	/**
	 * Creates proxy over the result Stream to ensures that close() call is always delegated to
	 * the close handler provided by the target ExecutionDelegate.
	 */
	private Stream<?> mixinWithCloseHandler(Stream<?> resultStream, Runnable closeHandler){
		resultStream.onClose(closeHandler);
		MethodInterceptor advice = new MethodInterceptor() {	
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				Object result = invocation.proceed();
				if (Stream.class.isAssignableFrom(invocation.getMethod().getReturnType())){
					Stream<?> stream = (Stream<?>) result;
					result = mixinWithCloseHandler(stream, closeHandler);
				}
				return result;
			}
		};
		return JvmUtils.proxy(resultStream, advice);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private R generateDistributable(Class<?> proxyType){	
		List<Class<?>> interfaces = new ArrayList<Class<?>>();
		if (DistributablePipeline.class.isAssignableFrom(proxyType)){
			interfaces.add(DistributablePipeline.class);
		} 
		else if (DistributableStream.class.isAssignableFrom(proxyType)){
			interfaces.add(DistributableStream.class);
		}
		else if (ExecutionGroup.class.isAssignableFrom(proxyType)){
			interfaces.add(ExecutionGroup.class);
		}
		else {
			throw new IllegalArgumentException("Unsupported proxy type: " +  proxyType);
		}
		interfaces.add(ExecutionConfigGenerator.class);
		
		R builderProxy = JvmUtils.proxy(new ArrayList(), this, interfaces.toArray(new Class[]{}));
		if (logger.isDebugEnabled()){
			logger.debug("Constructed builder proxy for " + interfaces);
		}
		return builderProxy;
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
			   operationName.equals("group") ||
			   operationName.equals("join") ||
			   operationName.equals("parallel");
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
		if (stages.size() > 0){
			return stages.get(stages.size()-1);
		}
		return null;
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
			if (stage.getDependentExecutionSpec() != null){
				ExecutionSpec dependentPipelineExecutionSpec = stage.getDependentExecutionSpec();
				this.setSourceSuppliers(executionProperties, dependentPipelineExecutionSpec.getStages(), dependentPipelineExecutionSpec.getName(), ename);
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
	private ExecutionSpecBuilder cloneTargetDistributable(){
		ExecutionSpecBuilder clonedDistributable = new  ExecutionSpecBuilder(this.sourceItemType, this.pipelineName, 
				this.targetDistributable instanceof DistributablePipeline ? DistributablePipeline.class : DistributableStream.class);
		((List)clonedDistributable.targetDistributable).addAll(Collections.unmodifiableList(((List)this.targetDistributable)));
		clonedDistributable.stageIdCounter = this.stageIdCounter;
		clonedDistributable.preExecCallbacks.addAll(Collections.unmodifiableList(this.preExecCallbacks));
		return clonedDistributable;
	}
}
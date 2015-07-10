package org.apache.dstream.tez;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * It implements {@link Callable} so it could be submitted to the {@link Executor} async.
 *
 * @param <T>
 */
public class TezDagExecutor implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(TezDagExecutor.class);
	
	private final ExecutionContextAwareTezClient tezClient;
	
	private final DAG dag;
	
//	private final OutputStreamsBuilder<?> outputBuilder;
	
	/**
	 * 
	 * @param tezClient
	 * @param dag
	 * @param outputBuilder
	 */
	public TezDagExecutor(ExecutionContextAwareTezClient tezClient, DAG dag) {
		this.tezClient = tezClient;
		this.dag = dag;
//		this.outputBuilder = outputBuilder;
	}

	@Override
	public void run() {
		if (logger.isInfoEnabled()){
			logger.info("Constructed Tez DAG " + dag.getName());
		}
		
		try {
			tezClient.waitTillReady();
		       
	        if (logger.isInfoEnabled()){
	        	logger.info("Submitting generated DAG to YARN/Tez cluster");
	        }
	 
	        DAGClient dagClient = tezClient.submitDAG(dag);

	        DAGStatus dagStatus =  dagClient.waitForCompletionWithStatusUpdates(null);
	        
	        if (logger.isInfoEnabled()){
	        	logger.info("DAG execution complete");
	        }
	        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	          logger.error("DAG diagnostics: " + dagStatus.getDiagnostics());
	        }
	        dagClient.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Failed to execute Tez DAG", e);
		}
        //return this.outputBuilder.build();
	}
}

package org.apache.dstream.tez;

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;

public class ExecutionContextAwareTezClient extends TezClient {
	private final  Map<String, LocalResource> localResources;

	private final TezConfiguration tezConfiguration;
	
	private final FileSystem fileSystem;

	/**
	 * 
	 * @param name
	 * @param tezConf
	 * @param localResources
	 * @param credentials
	 * @param fileSystem
	 */
	public ExecutionContextAwareTezClient(String name, 
            TezConfiguration tezConf,
            Map<String, LocalResource> localResources,
            Credentials credentials,
            FileSystem fileSystem) {
		
		super(name, tezConf, tezConf.getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT), localResources, credentials);
		this.tezConfiguration = tezConf;
		this.localResources = localResources;
		this.fileSystem = fileSystem;
	}
	
	public Map<String, LocalResource> getLocalResources() {
		return localResources;
	}

	public TezConfiguration getTezConfiguration() {
		return tezConfiguration;
	}
	
	public FileSystem getFileSystem() {
		return fileSystem;
	}
}

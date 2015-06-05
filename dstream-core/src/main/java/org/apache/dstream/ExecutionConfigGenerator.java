package org.apache.dstream;

/**
 * Strategy that is used to generate template execution configuration for all
 * instances of {@link DistributableExecutable}, by safely casting them to
 * {@link ExecutionConfigGenerator} and executing {@link #generateConfig()}
 * operation.<br>
 * 
 * <pre>
 * DistributableStream ds = . . .;
 * 
 * String configTemplate = ((ExecutionConfigGenerator)ds).generateConfig();
 * </pre>
 * 
 */
public interface ExecutionConfigGenerator {

	/**
	 * Generates configuration template for any instance of
	 * {@link DistributableExecutable}.<br>
	 * Simply cast it to {@link ExecutionConfigGenerator} and execute
	 * {@link #generateConfig()} operation.<br>
	 * 
	 * <pre>
	 * DistributableStream ds = . . .;
	 * 
	 * String configTemplate = ((ExecutionConfigGenerator)ds).generateConfig();
	 * </pre>
	 * 
	 * @return string representing template configuration for current instance
	 *         of {@link DistributableExecutable}
	 */
	String generateConfig();
}

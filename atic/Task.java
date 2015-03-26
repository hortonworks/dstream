package org.apache.dstream.assembly;

import java.io.Serializable;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 * @param <R>
 */
public abstract class Task<T,R> implements Serializable {
	private static final long serialVersionUID = -1917576454386721759L;
	
	protected Logger logger = LoggerFactory.getLogger(Task.class);

	/**
	 * 
	 * @param stream
	 * @param writer
	 */
	public abstract void execute(Stream<T> stream, Writer<R> writer);
}

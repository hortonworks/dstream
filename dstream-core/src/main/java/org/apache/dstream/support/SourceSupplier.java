package org.apache.dstream.support;

import org.apache.dstream.support.SerializableFunctionConverters.Supplier;


/**
 * Specialized definition of {@link Supplier} to return an array of sources of type T
 *
 * @param <T>
 */
public interface SourceSupplier<T> extends Supplier<T[]> {}

package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.DStream.DStream2.DStream2WithPredicate;
import org.apache.dstream.DStream.DStream3.DStream3WithPredicate;
import org.apache.dstream.DStream.DStream4.DStream4WithPredicate;
import org.apache.dstream.DStream.DStream5.DStream5WithPredicate;
import org.apache.dstream.function.SerializableFunctionConverters.SerPredicate;
import org.apache.dstream.utils.Assert;

/**
 * {@link Stream}-style specialization strategy which exposes distributable data  
 * as the sequence of elements of type A that support sequential and parallel 
 * aggregate operations.<br>
 * <br>
 * Below is the example of rudimentary <i>Word Count</i> written in this style:<br>
 * <pre>
 * DStream.ofType(String.class, "wc")
 *     .flatMap(line -> Stream.of(line.split("\\s+")))
 *     .reduceGroups(word -> word, word -> 1, Integer::sum)
 *  .executeAs("WordCount");
 * </pre>
 * 
 * It defines operations with the following classifications:<br>
 * <ol>
 * <li>
 * <i>intermediate</i> - similar to <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">intermediate operations</a>
 *    of Java {@link Stream}.<br>
 *    </li>
 * <li>
 * <i>terminal</i> - similar to <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal operations</a>
 *    of Java {@link Stream}.<br>
 *    </li>
 * <li>
 * <i>shuffle</i> - an operation which typically results in synchronization (typically data is written to  shuffle output).<br>
 *    </li>
 * <li>
 * <i>composable-shuffle</i> - an operation which typically results in synchronization if previous operation is <i>composable-transformation</i>.
 *    However, if previous operation is <i>composable-shuffle</i>, each subsequent operation that is also <i>composable-shuffle</i> could be composed
 *    into a single operation.<br>
 *    </li>
 * <li>
 * <i>composable-transformation</i> - an operation that defines a simple transformation (e.g., map, flatMap, filter etc.) and could be composed 
 *    with subsequent operations of the same type.<br>
 *    </li>
 * <ol>
 * <br>
 * @param <A> the type of the stream elements
 */
public interface DStream<A> extends BaseDStream<A, DStream<A>> {
		
	/**
	 * Factory method which creates an instance of the {@code DStream} of type T.
	 * 
	 * @param sourceItemType the type of the elements of this pipeline
	 * @param sourceIdentifier the value which will be used in conjunction with 
	 *                       {@link DStreamConstants#SOURCE} in configuration 
	 *                       to point to source of this stream 
	 *                       (e.g., dstream.source.foo=file://foo.txt where 'foo' is the <i>sourceIdentifier</i>)
	 *                       <b>Must be unique!</b>. If you simply want to point to the same source, map it 
	 *                       through configuration (e.g., dstream.source.foo=file://foo.txt, dstream.source.bar=file://foo.txt)
	 * @return the new {@link DStream} of type T
	 * 
	 * @param <T> the type of pipeline elements
	 */
	@SuppressWarnings("unchecked")
	public static <A> DStream<A> ofType(Class<A> sourceElementType, String sourceIdentifier) {	
		Assert.notNull(sourceElementType, "'sourceElementType' must not be null");
		Assert.notEmpty(sourceIdentifier, "'sourceIdentifier' must not be null or empty");
		return DStreamInvocationPipelineBuilder.as(sourceElementType, sourceIdentifier, DStream.class);
	}
	
	/**
	 * Will join two {@link DStream}s returning an instance of {@link DStream2}.
	 * <br>
	 * The actual instance of returned {@link DStream2} will be {@link DStream2WithPredicate}
	 * allowing predicate to be provided via {@link DStream2WithPredicate#on(SerPredicate)} operation.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-shuffle</i> operation.
	 * 
	 * @param <_A> the type of the {@link DStream} elements that are joined with this stream
	 * @param ds stream that is joined with this stream
	 * @return new {@link DStream2&lt;A,_A&gt;} representing the result of the join.
	 */
	<_A> DStream2WithPredicate<A,_A> join(DStream<_A> ds);
	
	/**
	 * Will join {@link DStream} with {@link DStream2} returning an instance of {@link DStream3}.
	 * <br>
	 * The actual instance of returned {@link DStream3} will be {@link DStream3WithPredicate}
	 * allowing predicate to be provided via {@link DStream3WithPredicate#on(SerPredicate)} operation.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-shuffle</i> operation.
	 * 
	 * @param <_A,_B> the type of the {@link DStream2} elements that are joined with this stream
	 * @param ds stream that is joined with this stream
	 * @return new {@link DStream3&lt;A,_A,_B&gt;} representing the result of the join.
	 */
	<_A,_B> DStream3WithPredicate<A,_A,_B> join(DStream2<_A,_B> ds);
	
	/**
	 * Will join {@link DStream} with {@link DStream3} returning an instance of {@link DStream4}. 
	 * <br>
	 * The actual instance of returned {@link DStream4} will be {@link DStream4WithPredicate}
	 * allowing predicate to be provided via {@link DStream4WithPredicate#on(SerPredicate)} operation.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-shuffle</i> operation.
	 * 
	 * @param <_A,_B,_C> the type of the {@link DStream3} elements that are joined with this stream
	 * @param ds stream that is joined with this stream
	 * @return new {@link DStream4&lt;A,_A,_B,_C&gt;} representing the result of the join.
	 */
	<_A,_B,_C> DStream4WithPredicate<A,_A,_B,_C> join(DStream3<_A,_B,_C> ds);
	
	/**
	 * Will join {@link DStream} with {@link DStream4} returning an instance of {@link DStream5}. 
	 * <br>
	 * The actual instance of returned {@link DStream5} will be {@link DStream5WithPredicate}
	 * allowing predicate to be provided via {@link DStream5WithPredicate#on(SerPredicate)} operation.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-shuffle</i> operation.
	 * 
	 * @param <_A,_B,_C,_D> the type of the {@link DStream4} elements that are joined with this stream
	 * @param ds stream that is joined with this stream
	 * @return new {@link DStream5&lt;A,_A,_B,_C,_D&gt;} representing the result of the join.
	 */
	<_A,_B,_C,_D> DStream5WithPredicate<A,_A,_B,_C,_D> join(DStream4<_A,_B,_C,_D> ds);
	
	
	/**
	 * Strategy which defines the type of {@link DStream} that contains two types of elements.
	 * 
	 * @param <A> - first element type
	 * @param <B> - second element type
	 */
	interface DStream2<A,B> extends BaseDStream<org.apache.dstream.utils.Tuples.Tuple2<A,B>, DStream2<A,B>> {
		
		/**
		 * Strategy which defines the type of {@link DStream2} that allows for predicate to be applied.
		 * 
		 * @param <A> - first element type
		 * @param <B> - second element type
		 */
		public interface DStream2WithPredicate<A,B> extends DStream2<A,B>{
			DStream2<A,B> on(SerPredicate<? super org.apache.dstream.utils.Tuples.Tuple2<A,B>> predicate);	
		}

		/**
		 * Will join {@link DStream2} with {@link DStream} returning an instance of {@link DStream3}. 
		 * <br>
		 * The actual instance of returned {@link DStream3} will be {@link DStream3WithPredicate}
		 * allowing predicate to be provided via {@link DStream3WithPredicate#on(SerPredicate)} operation.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>composable-shuffle</i> operation.
		 * 
		 * @param <_A> the type of the {@link DStream} element that is joined with this stream
		 * @param ds stream that is joined with this stream
		 * @return new {@link DStream3&lt;A,B,_A&gt;} representing the result of the join.
		 */
		<_A> DStream3WithPredicate<A,B,_A> join(DStream<_A> ds);
		
		/**
		 * Will join {@link DStream2} with {@link DStream2} returning an instance of {@link DStream4}. 
		 * <br>
		 * The actual instance of returned {@link DStream4} will be {@link DStream4WithPredicate}
		 * allowing predicate to be provided via {@link DStream4WithPredicate#on(SerPredicate)} operation.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>composable-shuffle</i> operation.
		 * 
		 * @param <_A,_B> the type of the {@link DStream2} elements that are joined with this stream
		 * @param ds stream that is joined with this stream
		 * @return new {@link DStream4&lt;A,B,_A,_B&gt;} representing the result of the join.
		 */
		<_A,_B> DStream4WithPredicate<A,B,_A,_B> join(DStream2<_A,_B> ds);
		
		/**
		 * Will join {@link DStream2} with {@link DStream3} returning an instance of {@link DStream5}. 
		 * <br>
		 * The actual instance of returned {@link DStream5} will be {@link DStream5WithPredicate}
		 * allowing predicate to be provided via {@link DStream5WithPredicate#on(SerPredicate)} operation.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>composable-shuffle</i> operation.
		 * 
		 * @param <_A,_B,_C> the type of the {@link DStream3} elements that are joined with this stream
		 * @param ds stream that is joined with this stream
		 * @return new {@link DStream5&lt;A,B,_A,_B,_C&gt;} representing the result of the join.
		 */
		<_A,_B,_C> DStream5WithPredicate<A,B,_A,_B,_C> join(DStream3<_A,_B,_C> ds);
	}
	
	/**
	 * Strategy which defines the type of {@link DStream} that contains three types of elements.
	 * 
	 * @param <A> - first element type
	 * @param <B> - second element type
	 * @param <C> - third element type
	 */
	interface DStream3<A,B,C> extends BaseDStream<org.apache.dstream.utils.Tuples.Tuple3<A,B,C>, DStream3<A,B,C>> {
		
		/**
		 * Strategy which defines the type of {@link DStream3} that allows for predicate to be applied.
		 * 
		 * @param <A> - first element type
		 * @param <B> - second element type
		 * @param <C> - third element type
		 */
		interface DStream3WithPredicate<A,B,C> extends DStream3<A,B,C>{
			DStream3<A,B,C> on(SerPredicate<? super org.apache.dstream.utils.Tuples.Tuple3<A,B,C>> predicate);	
		}

		/**
		 * Will join {@link DStream3} with {@link DStream} returning an instance of {@link DStream4}. 
		 * <br>
		 * The actual instance of returned {@link DStream4} will be {@link DStream4WithPredicate}
		 * allowing predicate to be provided via {@link DStream4WithPredicate#on(SerPredicate)} operation.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>composable-shuffle</i> operation.
		 * 
		 * @param <_A> the type of the {@link DStream} element that is joined with this stream.
		 * @param ds stream that is joined with this stream
		 * @return new {@link DStream4&lt;A,B,C,_A&gt;} representing the result of the join.
		 */
		<_A> DStream4WithPredicate<A,B,C,_A> join(DStream<_A> ds);
		
		/**
		 * Will join {@link DStream3} with {@link DStream2} returning an instance of {@link DStream5}. 
		 * <br>
		 * The actual instance of returned {@link DStream5} will be {@link DStream5WithPredicate}
		 * allowing predicate to be provided via {@link DStream5WithPredicate#on(SerPredicate)} operation.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>composable-shuffle</i> operation.
		 * 
		 * @param <_A,_B> the type of the {@link DStream2} elements that are joined with this stream.
		 * @param ds stream that is joined with this stream
		 * @return new {@link DStream5&lt;A,B,C,_A,_B&gt;} representing the result of the join.
		 */
		<_A,_B> DStream5WithPredicate<A,B,C,_A,_B> join(DStream2<_A,_B> ds);
	}
	
	/**
	 * Strategy which defines the type of {@link DStream} that contains four types of elements.
	 * 
	 * @param <A> - first element type
	 * @param <B> - second element type
	 * @param <C> - third element type
	 * @param <D> - third element type
	 */
	interface DStream4<A,B,C,D> extends BaseDStream<org.apache.dstream.utils.Tuples.Tuple4<A,B,C,D>, DStream4<A,B,C,D>> {
	
		/**
		 * Strategy which defines the type of {@link DStream4} that allows for predicate to be applied.
		 * 
		 * @param <A> - first element type
		 * @param <B> - second element type
		 * @param <C> - third element type
		 * @param <D> - third element type
		 */
		interface DStream4WithPredicate<A,B,C,D> extends DStream4<A,B,C,D>{
			DStream4<A,B,C,D> on(SerPredicate<? super org.apache.dstream.utils.Tuples.Tuple4<A,B,C,D>> predicate);	
		}

		/**
		 * Will join {@link DStream4} with {@link DStream} returning an instance of {@link DStream5}. 
		 * <br>
		 * The actual instance of returned {@link DStream5} will be {@link DStream5WithPredicate}
		 * allowing predicate to be provided via {@link DStream5WithPredicate#on(SerPredicate)} operation.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>composable-shuffle</i> operation.
		 * 
		 * @param <_A,_B> the type of the {@link DStream} element that is joined with this stream.
		 * @param ds stream that is joined with this stream
		 * @return new {@link DStream5&lt;A,B,C,_A,_B&gt;} representing the result of the join.
		 */
		<_A> DStream5WithPredicate<A,B,C,D,_A> join(DStream<_A> ds);
	}
	
	/**
	 * Strategy which defines the type of {@link DStream} that contains five types of elements.
	 * 
	 * @param <A> - first element type
	 * @param <B> - second element type
	 * @param <C> - third element type
	 * @param <D> - third element type
	 * @param <E> - third element type
	 */
	interface DStream5<A,B,C,D,E> extends BaseDStream<org.apache.dstream.utils.Tuples.Tuple5<A,B,C,D,E>, DStream5<A,B,C,D,E>> {
		/**
		 * Strategy which defines the type of {@link DStream5} that allows for predicate to be applied.
		 * 
		 * @param <A> - first element type
		 * @param <B> - second element type
		 * @param <C> - third element type
		 * @param <D> - third element type
		 * @param <E> - third element type
		 */
		interface DStream5WithPredicate<A,B,C,D,E> extends DStream5<A,B,C,D,E>{
			DStream5<A,B,C,D,E> on(SerPredicate<? super org.apache.dstream.utils.Tuples.Tuple5<A,B,C,D,E>> predicate);	
		}
	}
}

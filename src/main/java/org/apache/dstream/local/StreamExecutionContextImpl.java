package org.apache.dstream.local;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateKVResult;
import org.apache.dstream.IntermediateResult;
import org.apache.dstream.StageEntryPoint;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.dag.Stage;
import org.apache.dstream.io.FsStreamableSource;
import org.apache.dstream.io.ListStreamableSource;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.utils.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <T>
 */
public class StreamExecutionContextImpl<T> extends StreamExecutionContext<T> implements StageEntryPoint<T> {
	
	private final Logger logger = LoggerFactory.getLogger(StreamExecutionContextImpl.class);
	
	private final String[] supportedProtocols = new String[]{"file"};

	@Override
	protected boolean isSourceSupported(StreamableSource<T> source) {
		if (source instanceof ListStreamableSource){
			return true;
		} else if (source instanceof FsStreamableSource) {
			@SuppressWarnings("rawtypes")
			String protocol = ((FsStreamableSource)source).getScheme();
			for (String supportedProtocol : this.supportedProtocols) {
				if (supportedProtocol.equals(protocol)){
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public StreamableSource<T> getSource() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream toInputStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<T> stream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int computeInt(SerializableFunction<Stream<T>, Integer> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long computeLong(SerializableFunction<Stream<T>, Long> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double computeDouble(SerializableFunction<Stream<T>, Double> function) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean computeBoolean(
			SerializableFunction<Stream<T>, Boolean> function) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <R> IntermediateResult<R> computeCollection(
			SerializableFunction<Stream<T>, Collection<R>> function) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> IntermediateKVResult<K, V> computePairs(SerializableFunction<Stream<T>, Map<K, V>> function) {
		if (logger.isDebugEnabled()){
			logger.debug("Accepted 'computePairs' request");
		}
		
		Stage stage = new Stage(function);
		this.dagContext.addStage(stage);
	
		return new IntermediateKVResultImpl<K, V>();
	}

}

package cascading.redis;

import java.io.IOException;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Tuple;

import org.apache.hadoop.mapred.JobConf;

public abstract class RedisBaseScheme extends Scheme {

	@Override
	public boolean isSource() {
		return false;
	}
	
	@Override
	public void sinkInit(Tap tap, JobConf conf) throws IOException {
		
	}
	
	@Override
	public Tuple source(Object key, Object value) {
		throw new IllegalStateException("source should never be called");
	}

	@Override
	public void sourceInit(Tap tap, JobConf conf) throws IOException {
	
	}	
}

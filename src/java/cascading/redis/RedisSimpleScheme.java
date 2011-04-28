package cascading.redis;

import java.io.IOException;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import org.apache.hadoop.mapred.OutputCollector;

public class RedisSimpleScheme extends RedisBaseScheme {
	
	private String keyDelim = ":";
	private String valueDelim = ":";
	private Fields keyFields;
	private Fields valueFields;
	
	public RedisSimpleScheme(Fields keyFields, Fields valueFields) {
		this.keyFields = keyFields;
		this.valueFields = valueFields;
	}
	
	public RedisSimpleScheme(Fields keyFields, Fields valueFields, String keyDelim, String valueDelim) {
		this.keyFields = keyFields;
		this.valueFields = valueFields;
		this.keyDelim = keyDelim;
		this.valueDelim = valueDelim;
	}
	
	@Override
	public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
				
		String key = tupleEntry.selectTuple(keyFields).toString(keyDelim, false);
		String value = tupleEntry.selectTuple(valueFields).toString(valueDelim, false);
				
		outputCollector.collect(key, new RedisCommand("set", value));
	}
}

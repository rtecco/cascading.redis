package cascading.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import org.apache.hadoop.mapred.OutputCollector;

public class RedisHashScheme extends RedisBaseScheme {
	
	private String keyDelim = ":";
	private Fields keyFields;
	private Fields hashFields;
	
	public RedisHashScheme(Fields keyFields, Fields hashFields) {	
		this.keyFields = keyFields;
		this.hashFields = hashFields;
	}
	
	public RedisHashScheme(Fields keyFields, Fields hashFields, String keyDelim) {
		this.keyFields = keyFields;
		this.hashFields = hashFields;
		this.keyDelim = keyDelim;
	}
	
	@Override
	public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
				
		String key = tupleEntry.selectTuple(keyFields).toString(keyDelim, false);
		Map<String,String> value = new HashMap<String,String>();
		
		for(Comparable fieldName : hashFields) {			
			value.put((String)fieldName, tupleEntry.getString(fieldName));
		}
		
		outputCollector.collect(key, new RedisCommand("hmset", value));
	}
}

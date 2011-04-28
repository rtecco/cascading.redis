package cascading.redis;

import java.io.IOException;
import java.util.Map;

import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import org.apache.hadoop.mapred.OutputCollector;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisOutputCollector<V> extends TupleEntryCollector implements OutputCollector<String, V> {
	
	private JedisPool jedisPool;
	private Jedis jedis;
	
	public RedisOutputCollector(String hostname, int port) {
		jedisPool = new JedisPool(hostname, port);
		jedis = jedisPool.getResource();
	}
	
	@Override
	public void close() {
		jedisPool.returnResource(jedis);
		jedisPool.destroy();
	}
	
	@Override
	public void collect(String key, V value) throws IOException {
		
		RedisCommand cmd = (RedisCommand)value;
		
		// use reflection here someday? dunno.
		if(cmd.getCommand().equals("hmset")) {
			jedis.hmset(key, (Map)cmd.getValue());
		}
		
		else if(cmd.getCommand().equals("set")) {
			jedis.set(key, (String)cmd.getValue());
		}
		
		else {
			throw new IllegalArgumentException("unknown redis command");
		}
	}
	
	@Override
	protected void collect(Tuple tuple) {
	
	}
}

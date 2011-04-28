package cascading.redis;

import java.io.IOException;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import redis.clients.jedis.Jedis;

public class RedisTest extends ClusterTestCase {
	
	String inputFile = "src/test/data/test.txt";
	
	private Jedis getClient() {
		return new Jedis("localhost");
	}
		
	public RedisTest() {	
		super("redis sink tests");
	}
	
	public void setUp() {
		getClient().flushAll();
	}
	
	public void testHashSchemeSingleKeyField() {
		
		runTest(new RedisHashScheme(new Fields("key1"), new Fields("field1", "field2", "field3")));
		
		Jedis client = getClient();
		
		assertEquals("value11", client.hget("key11", "field1"));
		assertEquals("value12", client.hget("key11", "field2"));
		assertEquals("value13", client.hget("key11", "field3"));
		
		assertEquals("value21", client.hget("key21", "field1"));
		
		assertEquals("value33", client.hget("key31", "field3"));
	}
	
	public void testHashSchemeMultipleKeyFields() {
		
		runTest(new RedisHashScheme(new Fields("key1", "key2"), new Fields("field1", "field2"), "-"));
		
		Jedis client = getClient();
		
		assertEquals("value11", client.hget("key11-key12", "field1"));
		assertEquals("value12", client.hget("key11-key12", "field2"));
		assertNull(client.hget("key11-key12", "field3"));
		
		assertEquals("value22", client.hget("key21-key22", "field2"));
		
		assertEquals("value31", client.hget("key31-key32", "field1"));
		assertNull(client.hget("key31-key32", "field3"));
	}
	
	public void testSimpleSchemeMultipleKeyFields() {
		
		runTest(new RedisSimpleScheme(new Fields("key1", "key2"), new Fields("field1", "field3"), ":", "/"));
		
		Jedis client = getClient();
		
		assertEquals("value11/value13", client.get("key11:key12"));
		assertEquals("value21/value23", client.get("key21:key22"));
		assertEquals("value31/value33", client.get("key31:key32"));
	}
	
	private void runTest(RedisBaseScheme scheme) {
		
		Tap source = new Hfs(new TextDelimited(new Fields("key1", "key2", "field1", "field2", "field3"), " "), inputFile);
		Tap sink = new RedisSinkTap("localhost", scheme);
		Flow flow = new FlowConnector(getProperties()).connect(source, sink, new Pipe("identity"));
		
		flow.complete();
	}
}

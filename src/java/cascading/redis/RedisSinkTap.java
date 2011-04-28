package cascading.redis;

import java.io.IOException;

import cascading.tap.SinkTap;
import cascading.tuple.TupleEntryCollector;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class RedisSinkTap extends SinkTap {
	
	String hostname = null;
	int port = 6379;
	
	public RedisSinkTap(String hostname, RedisBaseScheme scheme) {
		
		super(scheme);
		
		this.hostname = hostname;
	}
		
	public RedisSinkTap(String hostname, int port, RedisBaseScheme scheme) {
		
		super(scheme);
		
		this.hostname = hostname;
		this.port = port;
	}
	
	@Override
	public boolean deletePath(JobConf conf) throws IOException {
		return true;
	}
	
	@Override
	public Path getPath() {
		return new Path("redis:/" + hostname + ":" + port);
	}
	
	@Override
	public long getPathModified(JobConf conf) throws IOException {
		return 0;
	}
	
	@Override
	public boolean isWriteDirect() {
		return true;
	}
	
	@Override
	public boolean makeDirs(JobConf conf) throws IOException {
		return true;
	}
	
	public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
		return new RedisOutputCollector(hostname, port);
	}
		
	@Override
	public boolean pathExists(JobConf conf) throws IOException {
		return true;
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(obj == null) {
			return false;
		}
		
		if(obj == this) {
			return true;
		}
		
		if(obj.getClass() != getClass()) {
			return false;
		}
		
		RedisSinkTap redisSinkTap = (RedisSinkTap)obj;
		
		return new EqualsBuilder()
			.appendSuper(super.equals(obj))
			.append(port, redisSinkTap.port)
			.append(hostname, redisSinkTap.hostname).
			isEquals();
	}
	
	@Override
	public int hashCode() {
		
		return new HashCodeBuilder(19, 69).
			appendSuper(super.hashCode()).
			append(port).
			append(hostname).
			toHashCode();
	}
	
	@Override
	public String toString() {
		
		if(hostname == null) {
			return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
		}
		
		else {
			return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + hostname + ":" + port + "\"]";
		}
	}
}

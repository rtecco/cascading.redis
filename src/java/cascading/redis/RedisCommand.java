package cascading.redis;

public class RedisCommand {
	
	private String cmd;
	private Object value;
	
	public RedisCommand(String cmd, Object value) {
		this.cmd = cmd;
		this.value = value;
	}
	
	public String getCommand() {
		return cmd;
	}
	
	public Object getValue() {
		return value;
	}
}

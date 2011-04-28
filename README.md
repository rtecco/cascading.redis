
# What

This is an experimental [Cascading](http://github.com/cwensel/cascading "Cascading") Sink to [Redis](http://github.com/antirez/redis "Redis").

It uses the excellent [Jedis](http://github.com/xetorthio/jedis "Jedis") client for most of its work.

The skeleton (incl. the build.xml) is copied from [cascading/memcached](http://github.com/cwensel/cascading.memcached "cascading.memcached").

cascading.redis is pretty minimal. It supports:

- sinking to hashes (RedisHashScheme)
- sinking simple key / value string pairs (RedisSimpleScheme)

# Building

This release requires at least Cascading 1.2.x. Hadoop 0.19.x or later, and a Redis cluster.

To build a jar,

> ant -Dcascading.home=... -Dhadoop.home=... jar

To test (requires a localhost Redis instance),

> ant -Dcascading.home=... -Dhadoop.home=... test

# Using

Add the .jar to the lib/ directory of your Hadoop application jar (like a normal dep).

# TODO

Jedis/Redis are extremely feature rich and coupled with the flexibility of Cascading, there's many possibilities.

- misc. cleanups, better logging and error handling (some sort of automatic retry like the memcached client?)
- license-ify
- a scheme for sets, sorted sets and other datatypes
- a scheme for pubsub. It would be neat to have Redis clients listening for data updates pushed via Cascading.
- support sourcing from Redis? Redis can usually be about 80% of a analytics / moments server.
- remove the RedisCommand object and use something fun like reflection?

package com.demo.hbase;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTools {
	private static JedisPool pool = null;

	static {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(10000);
		if (pool == null) {
			pool = new JedisPool(config, "localhost", 6379, 0);
		}
	}

	public static Jedis getJedis() {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
		} catch (Exception e) {
			jedis.close();
		}
		return jedis;
	}

	public static List<String> getOrderByPage(Jedis jedis, String key, Integer page, Integer count) {
		return jedis.lrange(key, page * count, page * count + count - 1);
	}

}

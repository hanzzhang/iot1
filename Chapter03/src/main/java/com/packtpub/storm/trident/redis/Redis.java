package com.packtpub.storm.trident.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class Redis {
	static final String host = "hanzredis1.redis.cache.windows.net";
	static final String password = "eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=";

	static public void flushDB() {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.flushDB();
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static public String get(String key) {
		String value = null;
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			value = jedis.get(key);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
		return value;
	}

	static public void set(String key, String value) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.set(key, value);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static public List<String> getList(String key, int maxLength) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		List<String> stringList = null;
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			stringList = jedis.lrange(key, 0, maxLength - 1);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
		return stringList;
	}

	static public void setList(String key, List<String> stringList) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.del(key);
			for (String str : stringList) {
				jedis.lpush(key, str);
			}
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static void putMap(String key, Map<String, String> map) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.hmset(key, map);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static Map<String, String> getMap(String key) {
		Map<String, String> map = null;
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			map = jedis.hgetAll(key);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
		return map;
	}

}

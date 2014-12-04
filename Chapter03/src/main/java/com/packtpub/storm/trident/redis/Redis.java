package com.packtpub.storm.trident.redis;

import java.util.List;

import redis.clients.jedis.Jedis;

public class Redis {
	static final String host = "hanzredis1.redis.cache.windows.net";
	static final String password = "eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=";
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
			for (String str : stringList) {
				jedis.lpush(key, str);
			}
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}
}
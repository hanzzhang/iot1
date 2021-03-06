package com.packtpub.storm.trident.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class RedisJava {
	// static String host = "localhost";
	static final String host = "hanzredis1.redis.cache.windows.net";

	public static void main(String[] args) {
		flushDB();
		// write();
		// read();
		// readlists("alerts", 1000);
	}

	static void flushDB() {
		String host = "hanzredis1.redis.cache.windows.net";
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth("eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=");
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.flushDB();
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static void write() {
		String host = "hanzredis1.redis.cache.windows.net";
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth("eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=");
		jedis.connect();
		if (jedis.isConnected()) {
			System.out.println("jedis.ping() result: " + jedis.ping());// PONG
			jedis.set("firstName", "My First name");
			jedis.lpush("citis", "San Fransisco");
			jedis.lpush("citis", "New Your");
			jedis.lpush("citis", "Seattle");
			jedis.lpush("citis", "Redmond");
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static void read() {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth("eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=");
		jedis.connect();
		if (jedis.isConnected()) {
			System.out.println("firstName:: " + jedis.get("firstName"));
			List<String> citis = jedis.lrange("citis", 0, 2);
			for (String city : citis) {
				System.out.println(city);
			}
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static void readlists(String listName, int sizeLimit) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth("eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=");
		jedis.connect();
		if (jedis.isConnected()) {
			List<String> lists = jedis.lrange(listName, 0, sizeLimit - 1);
			if (lists.isEmpty()) {
				System.out.println("there are no " + listName + " found.");
			}

			for (String item : lists) {
				System.out.println(item);
			}
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static void putMap(String key) {
			Map<String, String> userProperties = new HashMap<String, String>();
			userProperties.put("username", "Hanz Zhang");
			userProperties.put("firstName", "Hanz");
			userProperties.put("lastName", "Zhang");
			userProperties.put("email", "mshanz@hotmail.com");
			Redis.putMap(key, userProperties);
	}

	static void writeMap(String key) {
			Map<String, String> properties = Redis.getMap(key);
			System.out.println(properties.get("username"));
			System.out.println(properties.get("firstName"));
			System.out.println(properties.get("lastName"));
			System.out.println(properties.get("email"));
	}
	
	
}

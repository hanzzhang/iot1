package com.contoso.app.trident;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
public class Redis {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(Redis.class);
	public static void main(String[] args) {
		testList();
	}
	public static void testList(){
		String host = "hanzredis1.redis.cache.windows.net";
		String password = "eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=";
		List<String>li = new ArrayList<String>();
		li.add("1");
		li.add("2");
		li.add("3");
		li.add("4");	
		Redis.setList(host, password, "key", li);
		List<String>li1 = new ArrayList<String>();
		li1.add("1a");
		li1.add("2a");
		li1.add("3a");
		li1.add("4a");	
		Redis.setList(host, password, "key", li1);
		
		List<String>li2 = Redis.getList(host, password, "key", 50000);
		for(String s:li2){
			System.out.println(s);
		}	
	}

	static public String getHost(Properties properties) {
		logger.info("getHost Begin");

		String redisHost = properties.getProperty("redis.host");

		logger.info("getHost Returns " + redisHost);
		logger.info("getHost End");
		return redisHost;
	}

	static public String getPassword(Properties properties) {
		logger.info("getPassword Begin");

		String redisPassword = properties.getProperty("redis.password");

		logger.info("getPassword End");
		return redisPassword;
	}

	static public void flushDB(String host, String password) {
		logger.info("flushDB Begin");
		logger.info("flushDB params: host= " + host);

		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.flushDB();
		} else {
			logger.info("flushDB connection error !!!!!");
		}
		jedis.close();

		logger.info("flushDB End");
	}

	static public String get(String host, String password, String key) {
		logger.info("get Begin");
		logger.info("get params: host= " + host + " key= " + key);

		String value = null;
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			value = jedis.get(key);
		} else {
			logger.info("get connection error !!!!!");
		}
		jedis.close();

		logger.info("get returns " + value);
		logger.info("get Begin");
		return value;
	}

	static public void set(String host, String password, String key, String value) {
		logger.info("set Begin");
		logger.info("set params: host= " + host + " key= " + key + " value= " + value);

		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.set(key, value);
		} else {
			logger.info("set connection error !!!!!");
		}
		jedis.close();

		logger.info("set End");
	}

	static public List<String> getList(String host, String password, String key, int maxLength) {
		logger.info("getList Begin");
		logger.info("getList params: host= " + host + " key= " + key + " maxLength= " + maxLength);

		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		List<String> stringList = null;
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			stringList = jedis.lrange(key, 0, maxLength - 1);
		} else {
			logger.info("getList connection error !!!!!");
		}
		jedis.close();

		for (String s : stringList) {
			logger.info("getList returns: " + s);
		}
		logger.info("getList End");
		return stringList;
	}

	static public void setList(String host, String password, String key, List<String> stringList) {
		logger.info("setList Begin");
		logger.info("getList params: host= " + host + " key= " + key);

		if (stringList == null || stringList.isEmpty()) {
			logger.info("setList params stringList is empty  !!!!!!");
		} else {
			for (String s : stringList) {
				logger.info("setList params stringlist: " + s);
			}

			Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
			jedis.auth(password);
			jedis.connect();
			if (jedis.isConnected()) {
				jedis.del(key);
				for (String str : stringList) {
					jedis.lpush(key, str);
				}
			} else {
				logger.info("setList connection error !!!!!");
			}
			jedis.close();
		}

		logger.info("setList End");
	}
}
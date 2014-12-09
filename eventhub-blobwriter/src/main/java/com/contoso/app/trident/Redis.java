package com.contoso.app.trident;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
public class Redis {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(Redis.class);
	
	static public String getHost(Properties properties) {
		String redisHost= properties.getProperty("redis.host");
		return redisHost;
	}

	static public String getPassword(Properties properties) {
		String redisPassword= properties.getProperty("redis.password");
		return redisPassword;
	}
	
	static public void flushDB(String host, String password) {
		logger.info("flushDB");
		System.out.println("flushDB");		

		logger.info(host);		
		System.out.println(host);		

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

	static public String get(String host, String password, String key) {
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

	static public void set(String host, String password, String key, String value) {
		logger.info("set");
		System.out.println("set");		

		logger.info(host);
		logger.info(key);
		logger.info(value);

		System.out.println(host);
		System.out.println(key);
		System.out.println(value);

		
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

	static public List<String> getList(String host, String password, String key, int maxLength) {
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

	static public void setList(String host, String password, String key, List<String> stringList) {
		logger.info("setList");
		System.out.println("setList");		
		
		logger.info(host);
		logger.info(key);
		if (stringList.isEmpty()) {
			logger.info(" !!!!!!! list is empty");
		} else {
			for (String s : stringList)
				logger.info(s);
		}

		System.out.println(host);
		System.out.println(key);
		if (stringList.isEmpty()) {
			System.out.println(" !!!!!!! list is empty");
		} else {
			for (String s : stringList)
				System.out.println(s);
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
			System.out.println("connection error");
		}
		jedis.close();
	}

	static void putMap(String host, String password, String key, Map<String, String> map) {
		logger.info("putMap");
		System.out.println("putMap");		

		logger.info(host);
		logger.info(key);
		if (map.isEmpty()) {
			logger.info(" !!!!!!! map is empty");
		} else {
			for (String s : map.keySet()){
				logger.info(s);
				logger.info(map.get(s));
			}
		}

		System.out.println(host);
		System.out.println(key);
		if (map.isEmpty()) {
			System.out.println(" !!!!!!! map is empty");
		} else {
			for (String s : map.keySet()){
				System.out.println(s);
				System.out.println(map.get(s));
			}
		}

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

	static Map<String, String> getMap(String host, String password, String key) {
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

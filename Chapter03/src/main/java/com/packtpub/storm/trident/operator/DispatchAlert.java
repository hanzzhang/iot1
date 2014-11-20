package com.packtpub.storm.trident.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DispatchAlert extends BaseFunction {
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String alert = (String) tuple.getValue(0);
		Logger logger = (Logger) LoggerFactory.getLogger(DispatchAlert.class);
		String alertString = "ALERT !!! ALERT !!! ALERT !!! [" + alert + "]";
		logger.info(alertString);
		writeToRedis(alertString);
		// System.exit(0);
	}
	void writeToRedis(String alertString){
		String host = "hanzredis1.redis.cache.windows.net";
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth("eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=");
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.lpush("alerts", alertString);
		} else {
			Logger logger = (Logger) LoggerFactory.getLogger(DispatchAlert.class);
			logger.error("cannot cannect to redis");
		}
		jedis.close();
	}
}

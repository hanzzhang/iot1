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
		logger.info("Hanz ALERT RECEIVED [" + alert + "]");
		logger.info("Hanz Dispatch the national guard!");

		String host = "hanzredis1.redis.cache.windows.net";
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth("eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=");
		jedis.connect();
		if (jedis.isConnected()) {
			System.out.println("jedis.ping() result: " + jedis.ping());// PONG
			jedis.lpush("alerts", alert);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
		// System.exit(0);
	}
	
}

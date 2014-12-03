package com.packtpub.storm.trident.operator;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

public class ByteAggregator extends BaseAggregator<BlobState> {

	static Logger logger = (Logger) LoggerFactory.getLogger(BlobState.class);
	private static final long serialVersionUID = 1L;
	private static final int maxBlockStringLength = 64; // 1024 * 1024 * 4 / 16;
	private long txid;
	private int partitionIndex;
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		this.partitionIndex = context.getPartitionIndex();
		super.prepare(conf, context);
	}

	public BlobState init(Object batchId, TridentCollector collector) {
		if (batchId instanceof TransactionAttempt) {
			this.txid = ((TransactionAttempt) batchId).getTransactionId();
		}
		return new BlobState(this.partitionIndex, this.txid);
	}

	public void aggregate(BlobState state, TridentTuple tuple, TridentCollector collector) {
		String newLine = tuple.getString(0) + "\r\n";
		if (state.blockdata.length() + newLine.length() <= maxBlockStringLength) {
			state.blockdata = state.blockdata + newLine;
		} else {
			// ToDo: add logic to handle block overflow
		}
	}

	public void complete(BlobState state, TridentCollector collector) {
		// TODO: Write block to azure storage
		state.persist();
		collector.emit(new Values(state.partitionIndex));
		// collector.emit(null);
	}
}

class BlobState {
	static int maxNumberOfBlocks = 3;
	String key_LastTxid;
	String key_LastBlobid;
	String key_LastBlockid;

	int partitionIndex;
	long txid;
	int blobid;
	int blockid;

	String blockdata;
	Logger logger;

	public BlobState(int partitionIndex, long txid) {
		logger = (Logger) LoggerFactory.getLogger(BlobState.class);

		this.partitionIndex = partitionIndex;
		this.txid = txid;
		this.blockdata = "";

		this.key_LastTxid = "LastTxid_" + String.valueOf(partitionIndex);
		this.key_LastBlobid = "LastBlobid_" + String.valueOf(partitionIndex);
		this.key_LastBlockid = "LastBlockid_" + String.valueOf(partitionIndex);

		long lastTxid = -1;
		int lastBlobid = 1;
		int lastBlockid = 1;
		String lastTxidStr = Redis.get(this.key_LastTxid);
		if (lastTxidStr != null) {
			lastTxid = Long.parseLong(lastTxidStr);
		}
		String lastBlockidStr = Redis.get(this.key_LastBlockid);
		if (lastBlockidStr != null) {
			lastBlockid = Integer.parseInt(lastBlockidStr);
		}
		String lastBlobidStr = Redis.get(this.key_LastBlobid);
		if (lastBlobidStr != null) {
			lastBlobid = Integer.parseInt(lastBlobidStr);
		}

		if (lastTxidStr == null) { // this is the very first time the topology is running
			this.blobid = 1;
			this.blockid = 1;
		} else if (txid == lastTxid) { // this is a replay, overwrite old block
			this.blobid = lastBlobid;
			this.blockid = lastBlockid;
		} else if (txid != lastTxid) { // this is a new batch
			if (lastBlockid < maxNumberOfBlocks) { // append new block to existing blob
				this.blobid = lastBlobid;
				this.blockid = lastBlockid + 1;
			} else { // if (lastBlockid == maxNumberOfBlocks) // create a new blob
				this.blobid = lastBlobid + 1;
				this.blockid = 1;
			}
		}
	}
	
	public void persist() {
		Redis.set(key_LastTxid, String.valueOf(this.txid));
		Redis.set(this.key_LastBlobid, String.valueOf(this.blobid));
		Redis.set(this.key_LastBlockid, String.valueOf(this.blockid));

		logger.info(this.key_LastTxid + " = " + this.txid);
		logger.info(this.key_LastBlobid + " = " + this.blobid);
		logger.info(this.key_LastBlockid + " = " + this.blockid);
	}
}

class Redis {
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
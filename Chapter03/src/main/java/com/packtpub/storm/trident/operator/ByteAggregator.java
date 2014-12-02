package com.packtpub.storm.trident.operator;

import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

public class ByteAggregator extends BaseAggregator<BlobState> {
	private static final long serialVersionUID = 1L;
	private static final int maxBlocksize = 1024; // 1024 * 1024 * 4;
	private long txid;
	private int partitionIndex;
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		this.partitionIndex = context.getPartitionIndex();
	}

	public BlobState init(Object batchId, TridentCollector collector) {
		if (batchId instanceof TransactionAttempt) {
			this.txid = ((TransactionAttempt) batchId).getTransactionId();
		}
		return new BlobState(this.partitionIndex, this.txid);
	}

	public void aggregate(BlobState state, TridentTuple tuple, TridentCollector collector) {
		String newLine = tuple.getString(0) + "\r\n";
		if (state.blockdata.length() + newLine.length() <= maxBlocksize) {
			state.blockdata = state.blockdata + newLine;
		} else {
			// ToDo: add logic to handle block overflow
		}
	}

	public void complete(BlobState state, TridentCollector collector) {
		// TODO: Write block to azure storage
		state.persist();
		// collector.emit(null);
	}
}

class BlobState {
	static int maxNumberOfBlocks = 50000;
	String key_LastTxid;
	String key_LastBlockid;
	String key_LastBlobname;

	long txid;
	String blobname;
	int blockid;
	String blockdata;

	public BlobState(int partitionIndex, long txid) {

		String lastTxidStr;
		String lastBlockIdString;
		String lastBlobname;
		long lastTxid;
		int lastBlockId;

		this.txid = txid;
		this.blockdata = "";

		this.key_LastTxid = "LastTxid";
		this.key_LastBlockid = String.valueOf(partitionIndex) + "_LastBlockid";
		this.key_LastBlobname = String.valueOf(partitionIndex) + "_LastBlobname";

		lastTxidStr = Redis.get(this.key_LastTxid);
		if (lastTxidStr == null) {
			lastTxid = 0;
		} else {
			lastTxid = Long.parseLong(lastTxidStr);
		}

		lastBlockIdString = Redis.get(this.key_LastBlockid);
		if (lastBlockIdString == null) {
			lastBlockId = 1;
		} else {
			lastBlockId = Integer.parseInt(lastBlockIdString);
		}

		lastBlobname = Redis.get(this.key_LastBlobname);
		if (lastBlobname == null) {
			lastBlobname = String.valueOf(10000000 + partitionIndex * 100000 + 1);
		}

		if (lastTxidStr == null) { // this is the very first time the topology is running
			this.blobname = String.valueOf(10000000 + partitionIndex * 100000 + 1);
			this.blockid = 1;
		} else if (txid == lastTxid) { // this is a replay, overwrite old block
			this.blobname = lastBlobname;
			this.blockid = lastBlockId;
		} else if (lastBlockId < maxNumberOfBlocks) { // append new block to existing blob
			this.blobname = lastBlobname;
			this.blockid = lastBlockId + 1;
		} else { // create a new blob
			this.blobname = String.valueOf(Integer.parseInt(lastBlobname) + 1);
			this.blockid = 1;
		}
	}
	public void persist() {
		Redis.set(key_LastTxid, String.valueOf(this.txid));
		Redis.set(this.key_LastBlobname, this.blobname);
		Redis.set(this.key_LastBlockid, String.valueOf(this.blockid));
	}
}

class Redis {
	static final String host = "hanzredis1.redis.cache.windows.net";
	static Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
	static final String password = "eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=";
	static public String get(String key) {
		String value = null;
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
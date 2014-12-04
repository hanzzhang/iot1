package com.packtpub.storm.trident.operator;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.packtpub.storm.trident.azure.BlobWriter;
import com.packtpub.storm.trident.redis.Redis;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

public class ByteAggregator extends BaseAggregator<BlobState> {

	static Logger logger = (Logger) LoggerFactory.getLogger(BlobState.class);
	private static final long serialVersionUID = 1L;
	private static final int maxBlockStringLength = 1024; // 1024 * 1024 * 4 / 16;
	private long txid;
	private int partitionIndex;
	private Properties properties;
	public ByteAggregator(Properties properties) {
		this.properties = properties;
	}
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
		String partitionStr = String.format("%03d", state.partitionIndex);
		String blobidStr = String.format("%05d", state.blobid);
		String blobname = "a/blobwriter/" + partitionStr + "/" + blobidStr;
		String blockidStr = String.format("%05d", state.blockid);

		logger.info("uploading...blobname = " + blobname + " blockid = " + blockidStr);
		BlobWriter.upload(this.properties, blobname, blockidStr, state.blockdata);
		state.persist();
		collector.emit(new Values(state.partitionIndex));
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

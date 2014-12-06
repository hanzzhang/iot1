package com.packtpub.storm.trident.operator;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.packtpub.storm.trident.azure.BlobWriter;
import com.packtpub.storm.trident.state.BlobState;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

public class ByteAggregator extends BaseAggregator<BlobState> {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlobState.class);
	private int maxBlockBytes = 1024; // 1024 * 1024 * 4 / 16;

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
		this.maxBlockBytes = getMaxBlockBytes();
		int maxNumberOfBlocks = getMaxNumberOfblocks();
		BlobState state = new BlobState(this.partitionIndex, this.txid, maxNumberOfBlocks);
		BlobWriter.remove(this.properties, state.blockIdStrFormat, state.block.blobname, state.block.blockidStr);
		return state;
	}

	public void aggregate(BlobState state, TridentTuple tuple, TridentCollector collector) {

		String tupleStr = tuple.getString(0);
		if (tupleStr != null && tupleStr.length() > 0) {
			String newLine = tupleStr + "\r\n";
			if (newLine.getBytes().length > this.maxBlockBytes) {
				throw new FailedException();
			}

			if ((state.block.blockdata + newLine).getBytes().length <= this.maxBlockBytes) {
				state.block.addData(newLine);
			} else { // upload data, and go to next block
				logger.info("Upload Block");
				state.block.upload(this.properties);
				state.needPersist = true;
				state.buildNextblock();
				state.block.addData(newLine);
			}
		}
	}

	public void complete(BlobState state, TridentCollector collector) {
		if (state.block.blockdata.length() > 0) {
			state.block.upload(this.properties);
			state.needPersist = true;
		}
		
		if (state.needPersist) {
			state.persist();
		}
		collector.emit(new Values(this.partitionIndex));
	}

	private int getMaxNumberOfblocks() {
		int maxNumberOfBlocks = 10;
		String maxNumberOfBlocksStr = properties.getProperty("storage.blob.block.number.max");
		if (maxNumberOfBlocksStr != null) {
			maxNumberOfBlocks = Integer.parseInt(maxNumberOfBlocksStr);
		}
		return maxNumberOfBlocks;
	}

	private int getMaxBlockBytes() {
		int maxBlockBytes = 1024;
		String maxBlockBytesStr = properties.getProperty("storage.blob.block.kb.max");
		if (maxBlockBytesStr != null) {
			maxBlockBytes = Integer.parseInt(maxBlockBytesStr) * 1024;
		}
		return maxBlockBytes;
	}
}

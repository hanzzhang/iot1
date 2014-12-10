package com.contoso.app.trident;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

public class ByteAggregator extends BaseAggregator<BlobState> {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = (Logger) LoggerFactory.getLogger(ByteAggregator.class);

	private long txid;
	private int partitionIndex;
	private Properties properties;

	public ByteAggregator(Properties properties) {
		logger.info("Constructor Begin");
		this.properties = properties;
		logger.info("Constructor End");
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		logger.info("prepare Begin");
		this.partitionIndex = context.getPartitionIndex();
		super.prepare(conf, context);
		logger.info("prepare End");
	}

	public BlobState init(Object batchId, TridentCollector collector) {
		logger.info("init Begin");
		if (batchId instanceof TransactionAttempt) {
			this.txid = ((TransactionAttempt) batchId).getTransactionId();
		}
		BlobState state = new BlobState(this.partitionIndex, this.txid, this.properties);
		// BlobWriter.remove(this.properties, state.blockIdStrFormat, state.block.blobname, state.block.blockidStr);
		logger.info("init End");
		return state;
	}

	public void aggregate(BlobState state, TridentTuple tuple, TridentCollector collector) {
		// Don't log here since there will be too many entries (possibly millions);
		// for debugging, change the Config.properties values for storage.blob.block.bytes.max to a value less than 1000
		// and then uncomment logging code in this method
		//
		// logger.info("aggregate Begin");
		//
		String tupleStr = tuple.getString(0);
		if (tupleStr != null && tupleStr.length() > 0) {
			String msg = tupleStr + "\r\n";
			if (state.block.isMessageSizeWithnLimit(msg)) {
				if (state.block.willMessageFitCurrentBlock(msg)) {
					state.block.addData(msg);
				} else { 
					// since the new msg will not fit into the current block, we will upload the current block, 
					// build next block, and the add the new msg to the next block
					state.block.upload(this.properties);
					state.needPersist = true;
					state.addToNextNewBlock(msg);
				}
			}else{
				// message size is not within the limit, skip the message
			}
		}
		// Don't log here since there will be too many entries (possibly millions);
		// logger.info("aggregate end");
	}
	public void complete(BlobState state, TridentCollector collector) {
		logger.info("complete Begin");
		if (state.block.blockdata.length() > 0) {
			state.block.upload(this.properties); // upload the last block in the batch
			state.needPersist = true;
		}

		if (state.needPersist) {
			state.persist();
		}
		collector.emit(new Values(1)); // just emit a value
		logger.info("complete End");
	}
}

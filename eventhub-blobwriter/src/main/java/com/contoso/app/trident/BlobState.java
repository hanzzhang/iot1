package com.contoso.app.trident;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobState {
	public Block block;
	public boolean needPersist = false;

	private int maxNumberOfBlocks = 6;
	private static final String blobidBlockidStrFormat = "%05d_%05d";
	private static final String blobNameFormat = "aaa/blobwriter/%05d/%05d";
	public String blockIdStrFormat = "%05d";
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlobState.class);
	private String redisHost = null;
	private String redisPassword = null;

	private String key_txid;
	private String key_blocklist;

	private int partitionIndex;
	private long txid;
	private List<String> blocklist;
	private int maxBlockBytes;// 1024 * 1024 * 4 / 16;

	public BlobState(int partitionIndex, long txid, Properties properties) {
		logger.info("Constructor Begin");
		this.partitionIndex = partitionIndex;
		this.txid = txid;
		this.maxNumberOfBlocks = getMaxNumberOfblocks(properties);
		this.maxBlockBytes = getMaxBlockBytes(properties);

		redisHost = Redis.getHost(properties);
		redisPassword = Redis.getPassword(properties);
		this.key_txid = "txid_" + String.valueOf(partitionIndex);
		this.key_blocklist = "blocklist_" + String.valueOf(partitionIndex);;
		this.blocklist = new ArrayList<String>();

		String lastTxidStr = Redis.get(redisHost, redisPassword, this.key_txid);
		if (lastTxidStr == null) { // the very first time the topology is running
			this.block = new Block();
		} else {
			long lastTxid = Long.parseLong(lastTxidStr);
			if (txid != lastTxid) { // a new batch, not a replay
				this.block = getNextNewBlock(getLastBlock());
			} else {// if(txid == lastTxid) a replay, overwrite old block
				this.block = getFirstBlock();
			}
		}
		buildBlock();
		logger.info("Constructor End");
	}

	public void addToNextNewBlock(String msg) {
		logger.info("addToNextBlock Begin");
		
		this.block = getNextNewBlock(this.block);
		buildBlock();
		this.block.addData(msg);	
		
		logger.info("addToNextBlock End");
	}

	public void persist() {
		logger.info("persist Begin");
		Redis.set(redisHost, redisPassword, this.key_txid, String.valueOf(this.txid));
		Redis.setList(redisHost, redisPassword, this.key_blocklist, this.blocklist);

		logger.info(this.key_txid + " = " + this.txid);
		for (String s : this.blocklist) {
			logger.info(this.key_blocklist + " = " + s);
		}
		logger.info("persist End");
	}

	private void buildBlock() {
		logger.info("buildBlock Begin");
		this.block.blockdata = new String("");
		this.block.blobname = String.format(blobNameFormat, this.partitionIndex, this.block.blobid);
		this.block.blockidStr = String.format(blockIdStrFormat, this.block.blockid);
		String blobidBlockidStr = String.format(blobidBlockidStrFormat, this.block.blobid, this.block.blockid);
		this.blocklist.add(blobidBlockidStr);
		logger.info("buildBlock End");
	}

	private Block getLastBlock() {
		logger.info("getLastBlock Begin");
		Block block = new Block();
		List<String> lastBlobidBlockidList = Redis.getList(redisHost, redisPassword, this.key_blocklist, 50000);
		if (lastBlobidBlockidList != null && lastBlobidBlockidList.size() > 0) {
			String blobidBlockidStr = lastBlobidBlockidList.get(0);
			for (String s : lastBlobidBlockidList) {
				if (s.compareTo(blobidBlockidStr) > 0) {// find the last block written in the last batch
					blobidBlockidStr = s;
				}
			}
			String[] strArray = blobidBlockidStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
		}
		logger.info("getLastBlock End");
		return block;
	}

	private Block getFirstBlock() {
		logger.info("getFirstBlock Begin");
		Block block = new Block();
		List<String> lastblocks = Redis.getList(redisHost, redisPassword, this.key_blocklist, 50000);
		if (lastblocks != null && lastblocks.size() > 0) {
			String blockStr = lastblocks.get(0);
			for (String s : lastblocks) {
				if (s.compareTo(blockStr) < 0) {// find the first block written in the last batch
					blockStr = s;
				}
			}
			String[] strArray = blockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
		}
		logger.info("getFirstBlock End");
		return block;
	}

	private Block getNextNewBlock(Block current) {
		logger.info("getNextNewBlock Begin");
		Block nextBlock = new Block();
		if (current.blockid < maxNumberOfBlocks) {
			nextBlock.blobid = current.blobid;
			nextBlock.blockid = current.blockid + 1;
		} else {
			nextBlock.blobid = current.blobid + 1;
			nextBlock.blockid = 1;
		}
		logger.info("getNextNewBlock End");
		return nextBlock;
	}

	private int getMaxNumberOfblocks(Properties properties) {
		logger.info("getMaxNumberOfblocks Begin");

		int maxNumberOfBlocks = 10;
		String maxNumberOfBlocksStr = properties.getProperty("storage.blob.block.number.max");
		if (maxNumberOfBlocksStr != null) {
			maxNumberOfBlocks = Integer.parseInt(maxNumberOfBlocksStr);
		}

		logger.info("getMaxNumberOfblocks returns " + maxNumberOfBlocks);
		logger.info("getMaxNumberOfblocks End");
		return maxNumberOfBlocks;
	}

	private int getMaxBlockBytes(Properties properties) {
		logger.info("getMaxBlockBytes Begin");
		int maxBlockBytes = 1024;
		String maxBlockBytesStr = properties.getProperty("storage.blob.block.bytes.max");
		if (maxBlockBytesStr != null) {
			maxBlockBytes = Integer.parseInt(maxBlockBytesStr);
		}
		logger.info("getMaxBlockBytes returns " + maxBlockBytes);
		logger.info("getMaxBlockBytes End");
		return maxBlockBytes;
	}

	public class Block {
		int blobid = 1;
		int blockid = 1;
		public String blobname;
		public String blockidStr;
		public String blockdata;

		public Block() {
			logger.info("Block Constructor Begin");

			blobid = 1;
			blockid = 1;
			blockdata = "";

			logger.info("Block Constructor End");
		}

		public void addData(String msg) {
			// Don't log here since there will be too many entries (possibly millions);
			// for debugging, change the Config.properties values for storage.blob.block.bytes.max to a value less than 1000
			// and then uncomment logging code in this method
			//
			// logger.info("Block.addData Begin");
			//

			blockdata = blockdata + msg;

			// logger.info("Block.addData End");
		}

		public boolean isMessageSizeWithnLimit(String msg) {
			boolean result = false;
			if (msg.getBytes().length <= maxBlockBytes) {
				result = true;
			}
			return result;
		}

		public boolean willMessageFitCurrentBlock(String msg) {
			boolean result = false;
			int byteSize = (blockdata + msg).getBytes().length;
			if (byteSize <= maxBlockBytes) {
				result = true;
			}
			return result;
		}

		public void upload(Properties properties) {
			logger.info("Block.upload Begin");
			BlobWriter.upload(properties, this.blobname, this.blockidStr, this.blockdata);
			logger.info("BlobState.upload End");
		}
	}
}

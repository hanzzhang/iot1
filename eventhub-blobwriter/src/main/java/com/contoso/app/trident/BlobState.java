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

	public BlobState(int partitionIndex, long txid, Properties properties) {
		this.partitionIndex = partitionIndex;
		this.txid = txid;
		this.maxNumberOfBlocks = getMaxNumberOfblocks(properties);
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
				this.block = getNextBlock(getLastBlock());
			} else {// if(txid == lastTxid) a replay, overwrite old block
				this.block = getFirstBlock();
			}
		}
		buildBlock();
	}

	public void buildNextblock() {
		this.block = getNextBlock(this.block);
		buildBlock();
	}

	public void persist() {
		Redis.set(redisHost, redisPassword, this.key_txid, String.valueOf(this.txid));
		Redis.setList(redisHost, redisPassword, this.key_blocklist, this.blocklist);

		logger.info(this.key_txid + " = " + this.txid);
		for (String s : this.blocklist) {
			logger.info(this.key_blocklist + " = " + s);
		}
	}

	private void buildBlock() {
		this.block.blockdata = new String("");
		this.block.blobname = String.format(blobNameFormat, this.partitionIndex, this.block.blobid);
		this.block.blockidStr = String.format(blockIdStrFormat, this.block.blockid);
		String blobidBlockidStr = String.format(blobidBlockidStrFormat, this.block.blobid, this.block.blockid);
		this.blocklist.add(blobidBlockidStr);
	}

	private Block getLastBlock() {
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
		return block;
	}

	private Block getFirstBlock() {
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
		return block;
	}

	private Block getNextBlock(Block current) {
		Block next = new Block();
		if (current.blockid < maxNumberOfBlocks) {
			next.blobid = current.blobid;
			next.blockid = current.blockid + 1;
		} else {
			next.blobid = current.blobid + 1;
			next.blockid = 1;
		}
		return next;
	}

	public class Block {
		int blobid = 1;
		int blockid = 1;
		public String blobname;
		public String blockidStr;
		public String blockdata;
		
		public Block(){
			blobid = 1;
			blockid = 1;
			blockdata = "";
		}
		public void addData(String data) {
			blockdata = blockdata + data;
		}
		
		public String getData() {
			return blockdata;
		}

		public void upload(Properties properties) {
			BlobWriter.upload(properties, this.blobname, this.blockidStr, this.blockdata);
		}
	}

	private int getMaxNumberOfblocks(Properties properties) {
		int maxNumberOfBlocks = 10;
		String maxNumberOfBlocksStr = properties.getProperty("storage.blob.block.number.max");
		if (maxNumberOfBlocksStr != null) {
			maxNumberOfBlocks = Integer.parseInt(maxNumberOfBlocksStr);
		}
		return maxNumberOfBlocks;
	}
	

}

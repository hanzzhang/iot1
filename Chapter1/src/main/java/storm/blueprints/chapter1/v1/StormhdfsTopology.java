package storm.blueprints.chapter1.v1;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import static storm.blueprints.utils.Utils.*;

public class StormhdfsTopology {

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
//	private static final String COUNT_BOLT_ID = "count-bolt";
//	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String STORM_HDFS_ID = "storm-hdfs-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) throws Exception {

		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
//		WordCountBolt countBolt = new WordCountBolt();
//		ReportBolt reportBolt = new ReportBolt();
		HdfsBolt hdfsBolt = createHdfsBolt();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);

		// SentenceSpout --> SplitSentenceBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);

		builder.setBolt(STORM_HDFS_ID, hdfsBolt).globalGrouping(SPLIT_BOLT_ID);

		
//		// SplitSentenceBolt --> WordCountBolt
//		builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
//
//		// WordCountBolt --> ReportBolt
//		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

		Config config = new Config();
		config.setNumWorkers(2);

		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			waitForSeconds(10);
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		}
	}

	static HdfsBolt createHdfsBolt() {
		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/foo/");

		HdfsBolt bolt = new HdfsBolt().withFsUrl("hdfs://localhost:54310").withFileNameFormat(fileNameFormat).withRecordFormat(format)
				.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
		return bolt;
	}
}

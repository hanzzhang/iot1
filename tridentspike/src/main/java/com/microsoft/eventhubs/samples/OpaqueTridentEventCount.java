package com.microsoft.eventhubs.samples;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import com.microsoft.eventhubs.spout.EventHubSpoutConfig;

import java.io.FileReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.eventhubs.trident.OpaqueTridentEventHubSpout;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class OpaqueTridentEventCount {
	public static void main(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig = readEHConfig(args);
		StormTopology topology = buildTopology(spoutConfig);
		int numWorkers = spoutConfig.getPartitionCount();
		submitTopology(args, topology, numWorkers);
	}

	static EventHubSpoutConfig readEHConfig(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig;
		Properties properties = new Properties();
		if (args.length > 1) {
			properties.load(new FileReader(args[1]));
		} else {
			properties.load(EventCount.class.getClassLoader().getResourceAsStream("Config.properties"));
		}
		String username = properties.getProperty("eventhubspout.username");
		String password = properties.getProperty("eventhubspout.password");
		String namespaceName = properties.getProperty("eventhubspout.namespace");
		String entityPath = properties.getProperty("eventhubspout.entitypath");
		String zkEndpointAddress = properties.getProperty("zookeeper.connectionstring");
		int partitionCount = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
		int checkpointIntervalInSeconds = Integer.parseInt(properties.getProperty("eventhubspout.checkpoint.interval"));
		int receiverCredits = Integer.parseInt(properties.getProperty("eventhub.receiver.credits"));
		System.out.println("Eventhub spout config: ");
		System.out.println("  username: " + username);
		System.out.println("  password: " + password);
		System.out.println("  namespaceName: " + namespaceName);
		System.out.println("  entityPath: " + entityPath);
		System.out.println("  zkEndpointAddress: " + zkEndpointAddress);
			System.out.println("  partition count: " + partitionCount);
		System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
		System.out.println("  receiver credits: " + receiverCredits);
		spoutConfig = new EventHubSpoutConfig(username, password, namespaceName, entityPath, partitionCount, zkEndpointAddress, checkpointIntervalInSeconds, receiverCredits);
		if (args.length > 0) {
			spoutConfig.setTopologyName(args[0]);
		}
		return spoutConfig;
	}

	static StormTopology buildTopology(EventHubSpoutConfig spoutConfig) throws Exception {
		TridentTopology topology = new TridentTopology();

		OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);
		TridentState state = topology.newStream("stream-" + spoutConfig.getTopologyName(), spout).parallelismHint(spoutConfig.getPartitionCount())
				.aggregate(new Count(), new Fields(new String[] { "partial-count" }))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields(new String[] { "partial-count" }), new Sum(), new Fields(new String[] { "count" }));

		state.newValuesStream().each(new Fields(new String[] { "count" }), new LoggingFilter("Hanz got count: ", 10000));
		return topology.build();
	}

	static void submitTopology(String[] args, StormTopology topology, int numWorkers) throws Exception {
		Config config = new Config();
		config.setDebug(false);
		config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1L);
		if ((args != null) && (args.length > 0)) {
			config.setNumWorkers(numWorkers);
			StormSubmitter.submitTopology(args[0], config, topology);
		} else {
			config.setMaxTaskParallelism(2);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("test", config, topology);
			Thread.sleep(5000000L);
			localCluster.shutdown();
		}
	}
}

class LoggingFilter extends BaseFilter {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(LoggingFilter.class);
	private final String prefix;
	private final long logIntervalMs;
	private long lastTime;

	public LoggingFilter(String prefix, int logIntervalMs) {
		this.prefix = prefix;
		this.logIntervalMs = logIntervalMs;
		this.lastTime = System.nanoTime();
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		long now = System.nanoTime();
		if (this.logIntervalMs < (now - this.lastTime) / 1000000L) {
			logger.info(this.prefix + tuple.toString());
			this.lastTime = now;
		}
		return false;
	}
}

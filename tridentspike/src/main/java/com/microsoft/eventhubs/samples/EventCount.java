package com.microsoft.eventhubs.samples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import com.microsoft.eventhubs.samples.bolt.GlobalCountBolt;
import com.microsoft.eventhubs.samples.bolt.PartialCountBolt;
import com.microsoft.eventhubs.spout.EventHubSpout;
import com.microsoft.eventhubs.spout.EventHubSpoutConfig;
import java.io.FileReader;
import java.util.Properties;

public class EventCount {
	protected EventHubSpoutConfig spoutConfig;
	protected int numWorkers;

	protected void readEHConfig(String[] args) throws Exception {
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
		System.out.println("  partition count: " + partitionCount);
		System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
		System.out.println("  receiver credits: " + receiverCredits);
		this.spoutConfig = new EventHubSpoutConfig(username, password, namespaceName, entityPath, partitionCount, zkEndpointAddress,
				checkpointIntervalInSeconds, receiverCredits);

		this.numWorkers = this.spoutConfig.getPartitionCount();
		if (args.length > 0) {
			this.spoutConfig.setTopologyName(args[0]);
		}
	}

	protected EventHubSpout createEventHubSpout() {
		EventHubSpout eventHubSpout = new EventHubSpout(this.spoutConfig);
		return eventHubSpout;
	}

	protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("EventHubsSpout", eventHubSpout, Integer.valueOf(this.spoutConfig.getPartitionCount())).setNumTasks(
				Integer.valueOf(this.spoutConfig.getPartitionCount()));

		topologyBuilder.setBolt("PartialCountBolt", new PartialCountBolt(), Integer.valueOf(this.spoutConfig.getPartitionCount()))
				.localOrShuffleGrouping("EventHubsSpout").setNumTasks(Integer.valueOf(this.spoutConfig.getPartitionCount()));

		topologyBuilder.setBolt("GlobalCountBolt", new GlobalCountBolt(), Integer.valueOf(1)).globalGrouping("PartialCountBolt")
				.setNumTasks(Integer.valueOf(1));

		return topologyBuilder.createTopology();
	}

	protected void submitTopology(String[] args, StormTopology topology) throws Exception {
		Config config = new Config();
		config.setDebug(false);

		config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1L);
		if ((args != null) && (args.length > 0)) {
			config.setNumWorkers(this.numWorkers);
			StormSubmitter.submitTopology(args[0], config, topology);
		} else {
			config.setMaxTaskParallelism(2);

			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("test", config, topology);

			Thread.sleep(5000000L);

			localCluster.shutdown();
		}
	}

	protected void runScenario(String[] args) throws Exception {
		readEHConfig(args);
		EventHubSpout eventHubSpout = createEventHubSpout();
		StormTopology topology = buildTopology(eventHubSpout);
		submitTopology(args, topology);
	}

	public static void main(String[] args) throws Exception {
		EventCount scenario = new EventCount();
		scenario.runScenario(args);
	}
}

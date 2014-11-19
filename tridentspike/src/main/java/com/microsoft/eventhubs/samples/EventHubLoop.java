package com.microsoft.eventhubs.samples;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.microsoft.eventhubs.bolt.EventHubBolt;
import com.microsoft.eventhubs.spout.EventHubSpout;

public class EventHubLoop extends EventCount {
	@Override
	protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("EventHubsSpout", eventHubSpout, Integer.valueOf(this.spoutConfig.getPartitionCount())).setNumTasks(
				Integer.valueOf(this.spoutConfig.getPartitionCount()));

		EventHubBolt eventHubBolt = new EventHubBolt(this.spoutConfig.getConnectionString(), this.spoutConfig.getEntityPath());

		int boltTasks = this.spoutConfig.getPartitionCount() * 50;
		topologyBuilder.setBolt("EventHubsBolt", eventHubBolt, Integer.valueOf(boltTasks)).localOrShuffleGrouping("EventHubsSpout")
				.setNumTasks(Integer.valueOf(boltTasks));

		return topologyBuilder.createTopology();
	}

	public static void main(String[] args) throws Exception {
		EventHubLoop scenario = new EventHubLoop();
		scenario.runScenario(args);
	}
}
package com.microsoft.eventhubs.samples;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.microsoft.eventhubs.spout.EventHubSpout;
import com.microsoft.eventhubs.trident.TransactionalTridentEventHubSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class TransactionalTridentEventCount extends EventCount {
	public static class LoggingFilter extends BaseFilter {
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

	@Override
	protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
		TridentTopology topology = new TridentTopology();

		TransactionalTridentEventHubSpout spout = new TransactionalTridentEventHubSpout(this.spoutConfig);
		TridentState state = topology
				.newStream("stream-" + this.spoutConfig.getTopologyName(), spout)
				.parallelismHint(this.spoutConfig.getPartitionCount())
				.aggregate(new Count(), new Fields(new String[] { "partial-count" }))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields(new String[] { "partial-count" }), new Sum(),
						new Fields(new String[] { "count" }));

		state.newValuesStream().each(new Fields(new String[] { "count" }), new LoggingFilter("got count: ", 10000));
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		TransactionalTridentEventCount scenario = new TransactionalTridentEventCount();
		scenario.runScenario(args);
	}
}

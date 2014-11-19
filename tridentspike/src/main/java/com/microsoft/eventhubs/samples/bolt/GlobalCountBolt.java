package com.microsoft.eventhubs.samples.bolt;

import backtype.storm.metric.api.IMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalCountBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(GlobalCountBolt.class);
	private long globalCount;
	private long globalCountDiff;
	private long lastMetricsTime;
	private long throughput;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map config, TopologyContext context) {
		this.globalCount = 0L;
		this.globalCountDiff = 0L;
		this.lastMetricsTime = System.nanoTime();
		context.registerMetric("GlobalMessageCount", new IMetric() {
			@Override
			public Object getValueAndReset() {
				long now = System.nanoTime();
				long millis = (now - GlobalCountBolt.this.lastMetricsTime) / 1000000L;
				GlobalCountBolt.this.throughput = (GlobalCountBolt.this.globalCountDiff / millis * 1000L);
				// Map values = new HashMap();
				HashMap<String, Long> values = new HashMap<String, Long>();
				values.put("global_count", Long.valueOf(GlobalCountBolt.this.globalCount));
				values.put("throughput", Long.valueOf(GlobalCountBolt.this.throughput));
				GlobalCountBolt.this.lastMetricsTime = now;
				GlobalCountBolt.this.globalCountDiff = 0L;
				return values;
			}
		}, ((Integer) config.get("topology.builtin.metrics.bucket.size.secs")).intValue());
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		int partial = ((Integer) tuple.getValueByField("partial_count")).intValue();
		this.globalCount += partial;
		this.globalCountDiff += partial;
		if ((this.globalCountDiff == partial) && (this.globalCount != this.globalCountDiff)) {
			logger.info("Current throughput (messages/second): " + this.throughput);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}

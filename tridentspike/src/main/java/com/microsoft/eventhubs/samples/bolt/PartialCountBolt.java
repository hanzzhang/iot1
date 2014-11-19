package com.microsoft.eventhubs.samples.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class PartialCountBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	// private static final Logger logger =
	// LoggerFactory.getLogger(PartialCountBolt.class);
	// private static final int PartialCountBatchSize = 1000;
	private int partialCount;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.partialCount = 0;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		this.partialCount += 1;
		if (this.partialCount == 1000) {
			collector.emit(new Values(new Object[] { Integer.valueOf(1000) }));
			this.partialCount = 0;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[] { "partial_count" }));
	}
}

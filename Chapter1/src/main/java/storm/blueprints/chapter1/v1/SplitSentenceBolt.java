package storm.blueprints.chapter1.v1;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word : words) {
			this.collector.emit(tuple, new Values(word));
		}
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));
	}
}

package com.malsolo.storm.umbrella;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5353321827668876440L;

	private HashMap<String, Long> counts;
	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		counts = new HashMap<>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = this.counts.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word, count));
	}

}

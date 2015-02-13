package com.malsolo.storm.umbrella;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PrinterBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2897564674529835669L;

	private HashMap<String, Long> counts;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		counts.put(word, count);
	}
	
	@Override
	public void cleanup() {
		System.out.println("Final count");
		System.err.println("\n\nIt's the FINAL COUNTDOWN\n\n");
		//counts.entrySet().stream().sorted(Comparator.comparingLong(Map.Entry::getKey));
	}

}

package com.malsolo.storm.applied.githubcommits;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmailExtractor extends BaseBasicBolt {

	private static final long serialVersionUID = -4350903232246692170L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("email"));
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String commit = input.getStringByField("commit");
		String[] parts = commit.split(" ");
		collector.emit(new Values(parts[1]));
	}

}

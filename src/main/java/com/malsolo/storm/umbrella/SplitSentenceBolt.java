package com.malsolo.storm.umbrella;

import java.util.Map;
import java.util.stream.Stream;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3953714722108685161L;
	
	private OutputCollector collector;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getStringByField("sentence");
		Stream.of(sentence.split(" ")).map(word -> collector.emit(new Values(word))).close();
	}
	
	public static void main(String[] args) {
		String sentence = "Hola Mudo";
		Stream.of(sentence.split(" ")).map(word -> word.toUpperCase()).forEach(System.out::println);
		
	}

}

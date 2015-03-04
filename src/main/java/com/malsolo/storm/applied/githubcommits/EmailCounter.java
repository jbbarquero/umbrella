package com.malsolo.storm.applied.githubcommits;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class EmailCounter extends BaseBasicBolt {

	private static final long serialVersionUID = -937664638810898424L;
	
	private Map<String, Integer> counts;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// This bolt does not emit
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		counts = new HashMap<String, Integer>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String email = input.getStringByField("email");
		counts.put(email, countFor(email) + 1);
		printCounts();

	}

	private Integer countFor(String email) {
		Integer count = counts.get(email);
		return count == null ? 0 : count;
	}

	private void printCounts() {
		for (String email : counts.keySet()) {
			System.out.println(String.format("%s has count of %s", email,
					counts.get(email)));
		}
	}

}

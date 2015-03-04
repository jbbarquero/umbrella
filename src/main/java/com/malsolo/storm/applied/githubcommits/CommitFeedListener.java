package com.malsolo.storm.applied.githubcommits;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CommitFeedListener extends BaseRichSpout {

	private static final long serialVersionUID = 8284535672432040193L;

	private static final String COMMITS_FILE = "changelog.txt";

	private SpoutOutputCollector outputCollector;
	private List<String> commits;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("commit"));
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.outputCollector = collector;

		try {
			commits = IOUtils.readLines(
					ClassLoader.getSystemResourceAsStream(COMMITS_FILE),
					Charset.defaultCharset().name());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void nextTuple() {
		for (String commit : commits) {
			outputCollector.emit(new Values(commit));
		}
	}

}

package com.mapr.examples;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

public class TransferTool {

	private Producer producer;

	private Consumer consumer;

	private String fromTopic;

	private String toTopic;

	public TransferTool(String fromTopic, String toTopic) {

		this.fromTopic = fromTopic;
		this.toTopic = toTopic;
		producer = new Producer();
		consumer = new Consumer();
		
	}

	public void transfer() {
		
		consumer.setTopic(fromTopic);
		while (true) {
			List<String> msgs = consumer.consume();
			if (CollectionUtils.isNotEmpty(msgs)) {
				producer.send(toTopic, msgs);
			}
		}
	}

	public static void main(String[] args) {

		TransferTool tool = new TransferTool("from_topic", "to_topic");
		tool.transfer();
	}

}

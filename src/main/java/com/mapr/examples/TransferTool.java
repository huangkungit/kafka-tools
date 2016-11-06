package com.mapr.examples;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

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
		if(args == null || args.length < 3){
			System.out.println("the args is not enough!!");
		}
        String fromTopic = args[1];
        String toTopic = args[2];
		TransferTool tool = new TransferTool(fromTopic, toTopic);
		System.out.println("start trasfer");
		tool.transfer();
	}

}

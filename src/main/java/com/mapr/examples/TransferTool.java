package com.mapr.examples;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Maps;

public class TransferTool<K, V> {

	private Producer<K, V> producer;

	private Consumer<K, V> consumer;

	private String fromTopic;

	private String toTopic;

	private static final String BYTE = "byte";

	private static final String STRING = "string";

	private Map<String, Object> producerConfig;

	private Map<String, Object> consumerConfig;

	public TransferTool(String fromTopic, String toTopic, String type) {

		this.fromTopic = fromTopic;
		this.toTopic = toTopic;
		initial(type);
		producer = new Producer<>(producerConfig);
		consumer = new Consumer<>(consumerConfig);
	}

	public void transfer() {

		consumer.setTopic(fromTopic);
		while (true) {
			List<V> msgs = consumer.consume();
			if (CollectionUtils.isNotEmpty(msgs)) {
				producer.send(toTopic, msgs);
			}
		}
	}

	public void initial(String type) {

		producerConfig = Maps.newHashMap();
		consumerConfig = Maps.newHashMap();

		if (BYTE.equals(type)) {
			producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		} else {
			producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		}
	}

	public static void main(String[] args) {

		if (args == null || args.length < 4) {
			System.out.println("the args is not enough!!");
			System.exit(-1);
		}
		String fromTopic =  args[2];
		String toTopic = args[3];
		String type = args[1];
		TransferTool<String, byte[]> tool = new TransferTool<String, byte[]>(fromTopic, toTopic, type);
		System.out.println("start trasfer msg from " + fromTopic + " to " + toTopic);
		tool.transfer();
	}

}

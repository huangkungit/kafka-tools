package com.mapr.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alibaba.fastjson.JSON;
import com.google.common.io.Resources;

public class Producer {
	
	private KafkaProducer<String, String> producer;
	
	public Producer(){
		try {
			initial();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	private void initial() throws IOException {
		
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		}
	}
	
	
	private void process(String topic){
		try {
			for (int i = 100; i < 200; i++) {
				// send lots of messages
				producer.send(new ProducerRecord<String, String>(topic,generateMsg(i)),
						new Callback() {
							public void onCompletion(RecordMetadata metadata, Exception e) {
								System.out.println("The offset of the record we just sent is: " + metadata.offset());
							}
						});
				producer.flush();
			}
		} catch (Throwable throwable) {
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			close();
		}
	}
	
   public void send(String topic, List<String> msgs){
	   
	   if(StringUtils.isEmpty(topic) || CollectionUtils.isEmpty(msgs)){
		   return;
	   }
	   for(String msg : msgs){
		   producer.send(new ProducerRecord<String, String>(topic,msg));
	   }
	   producer.flush();
   }
   private String generateMsg(int i){				
		Map<String, Object> map = new TreeMap<>();
		map.put("id", i);
		map.put("time", System.currentTimeMillis()/1000/1000);
		map.put("action", "process" + i);
		return JSON.toJSONString(map);
	}
	
	private void close(){
		producer.close();
	}

	public static void main(String[] args) {
		
		Producer producer = new Producer();		
		try {
			producer.process("from_topic");

		} catch (Throwable throwable) {
			throwable.printStackTrace();
		} finally {
			producer.close();
		}

	}
}

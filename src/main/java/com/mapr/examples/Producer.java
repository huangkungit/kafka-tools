package com.mapr.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

public class Producer<K, V> {
	
	private KafkaProducer<K, V> producer;
	
	public Producer(Map<String, Object> config){
		try {
			initial(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	private void initial(Map<String, Object> config) throws IOException {
		
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			properties.putAll(config);
			producer = new KafkaProducer<>(properties);
		}
	}
	
	
//	private void process(String topic){
//		try {
//			for (int i = 200; i < 300; i++) {
//				// send lots of messages
//				producer.send(new ProducerRecord<K, V>(topic,generateMsg(i)),
//						new Callback() {
//							public void onCompletion(RecordMetadata metadata, Exception e) {
//								System.out.println("The offset of the record we just sent is: " + metadata.offset());
//							}
//						});
//				producer.flush();
//			}
//		} catch (Throwable throwable) {
//			System.out.printf("%s", throwable.getStackTrace());
//		} finally {
//			close();
//		}
//	}
	
   public void send(String topic, List<V> msgs){
	   
	   if(StringUtils.isEmpty(topic) || CollectionUtils.isEmpty(msgs)){
		   return;
	   }
	   for(V msg : msgs){
		   producer.send(new ProducerRecord<K, V>(topic,msg));
	   }
	   producer.flush();
   }
//   private String generateMsg(int i){				
//		Map<String, Object> map = new TreeMap<>();
//		map.put("id", i);
//		map.put("time", System.currentTimeMillis()/1000/1000);
//		map.put("action", "process" + i);
//		return JSON.toJSONString(map);
//	}
	
	private void close(){
		producer.close();
	}

	public static void main(String[] args) {
		
		Producer<String,String> producer = new Producer<>(new HashMap());
		String topic = "simple_input1";
		try {
			//producer.process(topic);

		} catch (Throwable throwable) {
			throwable.printStackTrace();
		} finally {
			producer.close();
		}

	}
}

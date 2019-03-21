package com.bob.kafka.cunsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerService {
	
	volatile static boolean RUNNING =true;
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");//不同ID 可以同时订阅消息
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("foo", "bar" , "my-topic"));//订阅TOPIC
		try {
		    while(RUNNING) {//轮询
		        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
		        for (TopicPartition partition : records.partitions()) {
		            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
		            for (ConsumerRecord<String, String> record : partitionRecords) {
		     //可以自定义Handler,处理对应的TOPIC消息(partitionRecords.key())
		                System.out.println(record.offset() + ": " + record.value());
		            }
		            consumer.commitSync();//同步
		        }
		    }
		} finally {
		  consumer.close();
		} 
	}

}

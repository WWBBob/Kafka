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
		props.put("group.id", "test");//��ͬID ����ͬʱ������Ϣ
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("foo", "bar" , "my-topic"));//����TOPIC
		try {
		    while(RUNNING) {//��ѯ
		        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
		        for (TopicPartition partition : records.partitions()) {
		            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
		            for (ConsumerRecord<String, String> record : partitionRecords) {
		     //�����Զ���Handler,�����Ӧ��TOPIC��Ϣ(partitionRecords.key())
		                System.out.println(record.offset() + ": " + record.value());
		            }
		            consumer.commitSync();//ͬ��
		        }
		    }
		} finally {
		  consumer.close();
		} 
	}

}

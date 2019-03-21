package com.bob.kafka.provider;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProviderService {
	
	public static void main(String[] args) throws InterruptedException  {
	    Properties props = new Properties();
	    props.put("bootstrap.servers", "localhost:9092");
	    props.put("acks", "all");
	    props.put("retries ", 1);
	    props.put("buffer.memory", 33554432);
	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	     
	    Producer<String, String> producer = new KafkaProducer<>(props);
	    for(int i = 0; i < 100; i++)
	        producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
	    
	    Thread.sleep(1000000000);
	    producer.close(); 
	}

}

package com.bob.kafka.cunsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MsgConsumer {
    private static final String GROUP = "MsgConsumer";
    private static final List TOPICS = Arrays.asList("my-topic");

    /**
     * 自动提交offset
     */
    public void autoCommit() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());//key反序列化方式
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getCanonicalName());//value反系列化方式
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);//自动提交
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");//指定broker地址，来找到group的coordinator
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,this.GROUP);//指定用户组

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(TOPICS);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//100ms 拉取一次数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic: "+record.topic() + " key: " + record.key() + " value: " + record.value() + " partition: "+ record.partition());
            }
        }
    }

    /**
     * 手动提交offset
     */
    public void consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());//key反序列化方式
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getCanonicalName());//value反系列化方式
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);//手动提交
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.59.130:9092,192.168.59.131:9092,192.168.59.132:9092");//指定broker地址，来找到group的coordinator
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,this.GROUP);//指定用户组

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(TOPICS);//指定topic消费

        long i = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//100ms 拉取一次数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic: "+record.topic() + " key: " + record.key() + " value: " + record.value() + " partition: "+ record.partition());
                i ++;
            }

            if (i >= 100) {
                consumer.commitAsync();//手动commit
                i = 0;
            }
        }
    }

    public static void main(String[] args) {
        new MsgConsumer().autoCommit();
    }
}

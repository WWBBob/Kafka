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
     * �Զ��ύoffset
     */
    public void autoCommit() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());//key�����л���ʽ
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getCanonicalName());//value��ϵ�л���ʽ
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);//�Զ��ύ
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");//ָ��broker��ַ�����ҵ�group��coordinator
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,this.GROUP);//ָ���û���

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(TOPICS);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//100ms ��ȡһ������
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic: "+record.topic() + " key: " + record.key() + " value: " + record.value() + " partition: "+ record.partition());
            }
        }
    }

    /**
     * �ֶ��ύoffset
     */
    public void consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());//key�����л���ʽ
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getCanonicalName());//value��ϵ�л���ʽ
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);//�ֶ��ύ
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.59.130:9092,192.168.59.131:9092,192.168.59.132:9092");//ָ��broker��ַ�����ҵ�group��coordinator
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,this.GROUP);//ָ���û���

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(TOPICS);//ָ��topic����

        long i = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//100ms ��ȡһ������
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic: "+record.topic() + " key: " + record.key() + " value: " + record.value() + " partition: "+ record.partition());
                i ++;
            }

            if (i >= 100) {
                consumer.commitAsync();//�ֶ�commit
                i = 0;
            }
        }
    }

    public static void main(String[] args) {
        new MsgConsumer().autoCommit();
    }
}

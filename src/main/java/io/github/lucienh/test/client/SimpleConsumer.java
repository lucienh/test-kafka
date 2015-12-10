package io.github.lucienh.test.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by h on 15-12-8.
 */
public class SimpleConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("partition.assignment.strategy", "range");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(props);
        consumer.subscribe("test");
        boolean isRunning = true;
        while (isRunning) {
            //poll方法返回null,报错
            Map<String, ConsumerRecords<Integer, String>> records = consumer.poll(100);
            process(records);
        }

        consumer.close();

    }

    private static void process(Map<String, ConsumerRecords<Integer, String>> records) {
        System.out.println(records);
    }
}

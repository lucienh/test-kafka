package io.github.lucienh.test.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * @author Abhilash S T P
 */
public class SimpleProducer {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public SimpleProducer() {
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(properties);
    }

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        String topic = "test";
        String messageStr = "asdfasdfdf";
        for (int i = 0; i < 100; i++) {
            ProducerRecord<Integer, String> data = new ProducerRecord<Integer, String>(topic, UUID.randomUUID().toString());
            producer.send(data);
        }
        producer.close();

    }
}
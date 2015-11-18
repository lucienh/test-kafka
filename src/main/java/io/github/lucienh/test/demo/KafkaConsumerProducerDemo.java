package io.github.lucienh.test.demo;

/**
 * @author leicui bourne_cui@163.com
 */
public class KafkaConsumerProducerDemo {
    public static void main(String[] args) {

        final String zkConnect = "127.0.0.1:2181";
        final String groupId = "group1";
        final String topic = "test";
        String kafkaBroker = "127.0.0.1:9092";

        KafkaProducer producerThread = new KafkaProducer(topic, kafkaBroker);
        producerThread.start();

        KafkaConsumer consumerThread = new KafkaConsumer(topic, zkConnect, groupId);
        consumerThread.start();
    }
}
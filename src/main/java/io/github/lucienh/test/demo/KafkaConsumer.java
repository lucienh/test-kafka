package io.github.lucienh.test.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * @author leicui bourne_cui@163.com
 */
public class KafkaConsumer extends Thread {

    private final ConsumerConnector consumer;

    private final String topic;

    public KafkaConsumer(String topic, String zkConnect, String groupId) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zkConnect, groupId));
        this.topic = topic;
    }

    private ConsumerConfig createConsumerConfig(String zkConnect, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, Integer.valueOf(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("receive：" + new String(it.next().message()));
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
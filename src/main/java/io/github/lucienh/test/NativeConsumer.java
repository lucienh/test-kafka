package io.github.lucienh.test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by h on 15-11-18.
 */
public class NativeConsumer {

    private String topic = "test";

    private String group = "group";

    private String zookeeper = "127.0.0.1:2181";

    private ConsumerConnector getConsumerConnector() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", group);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        return Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public ConsumerIterator<String, String> iterator(ConsumerConnector consumer) {


        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        topicCountMap.put(topic, Integer.valueOf(1));

        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());

        Map<java.lang.String, List<KafkaStream<java.lang.String, java.lang.String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
                stringDecoder, stringDecoder);

        KafkaStream<java.lang.String, java.lang.String> stream = consumerMap.get(topic).get(0);

        return stream.iterator();

    }

    public void printMessage() {

        ConsumerConnector consumer = getConsumerConnector();

        ConsumerIterator<String, String> it = iterator(consumer);

        while (it.hasNext()) {

            MessageAndMetadata<String, String> messageAndMetaData = it.next();

            System.out.println(Thread.currentThread().getName() + " receive :" + messageAndMetaData.message());

        }

        consumer.shutdown();
    }

    public static void main(String[] args) {
        NativeConsumer consumer = new NativeConsumer();
        consumer.printMessage();
    }


}

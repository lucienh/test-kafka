package io.github.lucienh.test.client;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class ConsumerSample {

    public static void main(String[] args) {
        // specify some consumer properties
        Properties props = new Properties();
        props.put("zk.connect", "localhost:2181");
        props.put("zk.connectiontimeout.ms", "1000000");
        props.put("groupid", "test_group");

        // Create the connection to the cluster
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        // create 4 partitions of the stream for topic “test-topic”, to allow 4 threads to consume
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("test-topic", 4);

        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> topicMessageStreams =
                consumerConnector.createMessageStreams(map, stringDecoder, stringDecoder);
        List<KafkaStream<String, String>> streams = topicMessageStreams.get("test-topic");

        // create list of 4 threads to consume from each of the partitions
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // consume the messages in the threads
        for (final KafkaStream<String, String> stream : streams) {
            executor.submit(new Runnable() {
                public void run() {
                    for (MessageAndMetadata msgAndMetadata : stream) {
                        // process message (msgAndMetadata.message())
                        System.out.println("topic: " + msgAndMetadata.topic());
                        Message message = (Message) msgAndMetadata.message();
                        ByteBuffer buffer = message.payload();
                        byte[] bytes = new byte[message.payloadSize()];
                        buffer.get(bytes);
                        String tmp = new String(bytes);
                        System.out.println("message content: " + tmp);
                    }
                }
            });
        }

    }
}

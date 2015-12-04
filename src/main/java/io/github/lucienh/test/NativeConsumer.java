package io.github.lucienh.test;

import io.github.lucienh.test.kafka.ConsumerMetadata;
import io.github.lucienh.test.kafka.ProducerMetadata;
import io.github.lucienh.test.kafka.ZookeeperConnect;
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

    private String group = "group";


    private ZookeeperConnect zookeeperConnect;

    private ConsumerMetadata consumerMetadata;


    public void setZookeeperConnect(ZookeeperConnect zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public NativeConsumer() {
        zookeeperConnect = new ZookeeperConnect();
        String topic = "test4";
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
        consumerMetadata = new ConsumerMetadata();
        consumerMetadata.setGroupId(group);
        consumerMetadata.setTopic(topic);
        consumerMetadata.setKeyDecoder(stringDecoder);
        consumerMetadata.setValueDecoder(stringDecoder);
        consumerMetadata.setAutoCommitInterval("1000");

    }

    private ConsumerConnector getConsumerConnector() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperConnect.getZkConnect());
        props.put("group.id", group);
        props.put("zookeeper.session.timeout.ms", zookeeperConnect.getZkSessionTimeout());
        props.put("zookeeper.sync.time.ms", zookeeperConnect.getZkSyncTime());
        props.put("auto.commit.interval.ms", consumerMetadata.getAutoCommitInterval());

        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        return Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public ConsumerIterator<String, String> iterator(ConsumerConnector consumer) {


        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        topicCountMap.put(consumerMetadata.getTopic(), Integer.valueOf(1));


        Map<java.lang.String, List<KafkaStream<java.lang.String, java.lang.String>>> consumerMap = consumer
                .createMessageStreams(topicCountMap, consumerMetadata.getKeyDecoder(), consumerMetadata.getValueDecoder());

        KafkaStream<java.lang.String, java.lang.String> stream = consumerMap.get(consumerMetadata.getTopic()).get(0);

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

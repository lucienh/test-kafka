package io.github.lucienh.test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class NativeProducer {

    public static void main(String[] args) {
        String topic = "test";
        long events = 100;
        Random rand = new Random();

        Properties props = new Properties();
        //props.put("zk.connect", "127.0.0.1:2181");
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);



        for (long nEvents = 0; nEvents < events; nEvents++) {
            Producer<String, String> producer = new Producer<String, String>(config);
            String msg = "NativeMessage-" + rand.nextInt();
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, nEvents + "", msg);

            producer.send(data);
            System.out.println("Send:" + msg);
            producer.close();

        }
    }
}

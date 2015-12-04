package io.github.lucienh.test.simple;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Test the Kafka Producer
 * 
 * @author jcsong2
 *
 */
public class SimpleProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		//props.put("zk.connect", "127.0.0.1:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "127.0.0.1:9092");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i < 10; i++)
			producer.send(new KeyedMessage<String, String>("mytest", "mytest" + i));
	}
}
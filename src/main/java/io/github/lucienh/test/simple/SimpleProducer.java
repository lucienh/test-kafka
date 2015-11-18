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
		props.put("zk.connect", "192.168.36.133:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.36.133:9092");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i < 10; i++)
			producer.send(new KeyedMessage<String, String>("mytest", "mytest" + i));
	}
}
package io.github.lucienh.test.avro;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.DecoderException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;


public class ProducerTest {

    void producer(Schema schema) throws IOException {

        Properties props = new Properties();
        props.put("metadata.broker.list", "0:9092");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
        GenericRecord payload1 = new GenericData.Record(schema);
        //Step2 : Put data in that genericrecord object
        payload1.put("desc", "'testdata'");
        //payload1.put("name", "我们");
        payload1.put("name", "dbevent1");
        payload1.put("id", 111);
        System.out.println("Original Message : " + payload1);
        System.out.println("Original Message length is : " + payload1.toString().getBytes().length);
        //Step3 : Serialize the object to a bytearray
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(payload1, encoder);
        encoder.flush();
        out.close();

        byte[] serializedBytes = out.toByteArray();
        System.out.println("Sending message in bytes : " + serializedBytes);
        System.out.println("serializedBytes is " + serializedBytes.length);
        //String serializedHex = Hex.encodeHexString(serializedBytes);
        //System.out.println("Serialized Hex String : " + serializedHex);
        KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>("page_views", serializedBytes);
        producer.send(message);
        producer.close();

    }


    public static void main(String[] args) throws IOException, DecoderException {
        ProducerTest test = new ProducerTest();
        Schema schema = new Schema.Parser().parse(new File("src/test_schema.avsc"));
        test.producer(schema);
    }
}
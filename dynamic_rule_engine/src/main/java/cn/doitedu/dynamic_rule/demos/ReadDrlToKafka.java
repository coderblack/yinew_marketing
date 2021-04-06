package cn.doitedu.dynamic_rule.demos;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class ReadDrlToKafka {
    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hdp01:9092,hdp02:9092,hdp03:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        String s = FileUtils.readFileToString(new File("dynamic_rule_engine/src/main/resources/rules/flink.drl"), "utf-8");

        ProducerRecord<String, String> record = new ProducerRecord<>("test_drools", "rule1,"+s);
        kafkaProducer.send(record);
        kafkaProducer.flush();

    }
}

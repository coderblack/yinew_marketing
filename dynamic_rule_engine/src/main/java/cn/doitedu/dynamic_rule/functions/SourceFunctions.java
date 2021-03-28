package cn.doitedu.dynamic_rule.functions;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceFunctions {

    public static FlinkKafkaConsumer<String> getKafkaEventSource(){

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hdp01:9092,hdp02:9092,hdp03:9092");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>("yinew_applog", new SimpleStringSchema(), props);


        return source;
    }


}

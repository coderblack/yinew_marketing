package cn.doitedu.dynamic_rule.demos;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class ReadDrlToMySql {
    public static void main(String[] args) throws IOException, SQLException {

        String s = FileUtils.readFileToString(new File("dynamic_rule_engine/src/main/resources/rules/flink.drl"), "utf-8");


        Connection conn = DriverManager.getConnection("jdbc:mysql://hdp01:3306/realtimedw", "root", "ABC123abc.123");

        PreparedStatement pst = conn.prepareStatement("insert into test_drools (rule_name,rule_code) values (?,?)");
        pst.setString(1,"rule3");
        pst.setString(2,s);

        boolean execute = pst.execute();

        pst.close();
        conn.close();


    }
}

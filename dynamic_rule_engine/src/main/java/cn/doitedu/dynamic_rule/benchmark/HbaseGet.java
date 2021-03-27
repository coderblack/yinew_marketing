package cn.doitedu.dynamic_rule.benchmark;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HbaseGet {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hdp01:2181,hdp02:2181,hdp03:2181");

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("yinew_profile"));

        long s = System.currentTimeMillis();
        for(int i=0;i<1000;i++){
            Get get = new Get(StringUtils.leftPad(i + "", 6, "0").getBytes());
            get.addColumn("f".getBytes(),"tag25".getBytes());
            get.addColumn("f".getBytes(),"tag67".getBytes());
            get.addColumn("f".getBytes(),"tag27".getBytes());
            Result result = table.get(get);
            byte[] v1 = result.getValue("f".getBytes(), "tag25".getBytes());
            byte[] v2 = result.getValue("f".getBytes(), "tag67".getBytes());
            byte[] v3 = result.getValue("f".getBytes(), "tag27".getBytes());
        }

        long e = System.currentTimeMillis();
        System.out.println(e-s);
        conn.close();
    }
}

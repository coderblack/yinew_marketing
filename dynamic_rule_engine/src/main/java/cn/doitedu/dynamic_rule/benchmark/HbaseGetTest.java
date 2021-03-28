package cn.doitedu.dynamic_rule.benchmark;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseGetTest {
    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hdp01:2181,hdp02:2181,hdp03:2181");

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("yinew_profile"));


        long s = System.currentTimeMillis();
        for(int i=0;i<1000;i++){
            Get get = new Get(StringUtils.leftPad(RandomUtils.nextInt(1, 900000) + "", 6, "0").getBytes());
            int i1 = RandomUtils.nextInt(1, 100);
            int i2 = RandomUtils.nextInt(1, 100);
            int i3 = RandomUtils.nextInt(1, 100);
            get.addColumn("f".getBytes(), Bytes.toBytes("tag"+i1));
            get.addColumn("f".getBytes(), Bytes.toBytes("tag"+i2));
            get.addColumn("f".getBytes(), Bytes.toBytes("tag"+i3));


            Result result = table.get(get);
            byte[] v1 = result.getValue("f".getBytes(), Bytes.toBytes("tag" + i1));
            byte[] v2 = result.getValue("f".getBytes(), Bytes.toBytes("tag" + i2));
            byte[] v3 = result.getValue("f".getBytes(), Bytes.toBytes("tag" + i3));
        }
        long e = System.currentTimeMillis();

        System.out.println(e-s);
        conn.close();

    }

}

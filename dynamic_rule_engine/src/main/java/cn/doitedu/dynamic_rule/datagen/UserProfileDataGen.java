package cn.doitedu.dynamic_rule.datagen;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-27
 * @desc 用户画像数据模拟器
 *
 * deviceid,k1=v1
 *
 * hbase中需要先创建好画像标签表
 * [root@hdp01 ~]# hbase shell
 * hbase> create 'yinew_profile','f'
 *
 *
 */
public class UserProfileDataGen {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hdp01:2181,hdp02:2181,hdp03:2181");

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("yinew_profile"));

        ArrayList<Put> puts = new ArrayList<>();
        for(int i=1;i<10000;i++){

            // 攒满100条一批
            for(int j=0;j<100;j++) {

                // 生成一个用户的画像标签数据
                String deviceId = StringUtils.leftPad(i + "", 6, "0");
                Put put = new Put(Bytes.toBytes(deviceId));
                for (int k = 1; k <= 100; k++) {
                    String key = "tag" + k;
                    String value = "v" + RandomUtils.nextInt(1, 101);
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(value));
                }

                // 将这一条画像数据，添加到list中
                puts.add(put);
            }


            // 提交一批
            table.put(puts);
            puts.clear();

        }

        conn.close();
    }
}

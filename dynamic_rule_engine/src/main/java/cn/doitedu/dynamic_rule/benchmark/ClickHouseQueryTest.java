package cn.doitedu.dynamic_rule.benchmark;

import cn.doitedu.dynamic_rule.utils.ConnectionUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;

public class ClickHouseQueryTest {

    public static void main(String[] args) throws Exception {
        String sql = "select \n" +
                " deviceId,count() as cnt \n" +
                " from yinew_detail \n" +
                " where deviceId= ?\n" +
                " and \n" +
                " eventId = 'W' \n" +
                " and \n" +
                " timeStamp >= 0 and timeStamp <=9223372036854775807\n" +
                " and properties['p2']='v3'\n" +
                " group by deviceId";

        ArrayList<Long> ss = new ArrayList<>();
        ArrayList<Long> ee = new ArrayList<>();

        for (int i = 0; i < 1; i++) {
            new Thread(new Runnable() {
                Connection conn = ConnectionUtils.getClickhouseConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);

                @Override
                public void run() {
                    try {
                        long s = System.currentTimeMillis();
                        ss.add(s);
                        for (int i = 0; i < 1000; i++) {
                            stmt.setString(1, StringUtils.leftPad(i + "", 6, "0"));
                            ResultSet resultSet = stmt.executeQuery();
                            while (resultSet.next()) {
                                long cnt = resultSet.getLong(2);
                            }
                        }
                        long e = System.currentTimeMillis();
                        System.out.println(e-s);
                        ee.add(e);
                    } catch (Exception e) {
                    }
                }
            }).start();
        }

        Thread.sleep(8000);

        Collections.sort(ss);
        Collections.sort(ee);

        //System.out.println(ee.get(ee.size()-1) - ss.get(0));


    }
}

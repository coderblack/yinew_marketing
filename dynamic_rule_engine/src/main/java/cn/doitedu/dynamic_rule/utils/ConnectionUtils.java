package cn.doitedu.dynamic_rule.utils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-30
 * @desc 各类外部链接创建工具类
 */
@Slf4j
public class ConnectionUtils {


    public static Connection getClickhouseConnection() throws Exception {
        //String ckDriver = "com.github.housepower.jdbc.ClickHouseDriver";
        String ckDriver = "ru.yandex.clickhouse.ClickHouseDriver";
        String ckUrl = "jdbc:clickhouse://192.168.77.63:8123/default";
        String table = "yinew_detail";

        Class.forName(ckDriver);
        Connection conn = DriverManager.getConnection(ckUrl);
        log.debug("clickhouse jdbc 连接创建完成");
        return conn;
    }


}

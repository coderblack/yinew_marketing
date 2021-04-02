package cn.doitedu.log4j;

import org.apache.log4j.Logger;

public class Demo {

    public static void main(String[] args) {

        // 根logger
        Logger rootLogger = Logger.getRootLogger();

        // 每一个logger对象都有自己的命名
        Logger logger1 = Logger.getLogger("cn");

        // cn.doitedu这个logger是 cn这个logger的 子logger
        // 子logger会继承父logger的配置
        Logger logger2 = Logger.getLogger("cn.doitedu");


        Logger demoLogger = Logger.getLogger(Demo.class);


        // debug的级别最低
        logger1.debug("这是一条debug级别的日志");

        // info级别大于debug
        logger1.info("这是一条info级别的日志");

        // warn级别大于info
        logger1.warn("这是一条warn级别的日志");

        // error级别大于warn
        logger1.error("这是一条error级别的日志");


        logger2.debug("这是cn.doitedu 这个 logger的debug日志");
        logger2.info("这是cn.doitedu 这个 logger的info日志");
        logger2.warn("这是cn.doitedu 这个 logger的warn日志");
        logger2.error("这是cn.doitedu 这个 logger的error日志");


    }
}

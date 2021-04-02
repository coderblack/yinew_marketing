package cn.doitedu.dynamic_rule.demos;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
public class Slf4jLog4jDemo {

    public static void main(String[] args) throws InterruptedException {

        while(true) {
            log.debug("哈哈哈哈哈");
            log.info("哈哈哈哈哈");
            log.warn("哈哈哈哈哈");
            log.error("哈哈哈哈哈");

            Thread.sleep(1000);
        }

    }
}

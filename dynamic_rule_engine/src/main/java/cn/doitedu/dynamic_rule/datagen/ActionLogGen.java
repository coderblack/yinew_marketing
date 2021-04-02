package cn.doitedu.dynamic_rule.datagen;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-27
 * @desc 行为日志生成模拟器
 *
 * {
 * 	"account": "Vz54E9Ya",
 * 	"appId": "cn.doitedu.app1",
 * 	"appVersion": "3.4",
 * 	"carrier": "中国移动",
 * 	"deviceId": "WEISLD0235S0934OL",
 * 	"deviceType": "MI-6",
 * "ip": "24.93.136.175",
 * 	"latitude": 42.09287620431088,
 * 	"longitude": 79.42106825764643,
 * 	"netType": "WIFI",
 * 	"osName": "android",
 * 	"osVersion": "6.5",
 * 	"releaseChannel": "豌豆荚",
 * 	"resolution": "1024*768",
 * 	"sessionId": "SE18329583458",
 * 	"timeStamp": 1594534406220
 * 	"eventId": "productView",
 * 	"properties": {
 * 		"pageId": "646",
 * 		"productId": "157",
 * 		"refType": "4",
 * 		"refUrl": "805",
 * 		"title": "爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码",
 * 		"url": "https://item.jd.com/36506691363.html",
 * 		"utm_campain": "4",
 * 		"utm_loctype": "1",
 * 		"utm_source": "10"
 *        }
 *
 * }
 *
 *
 * kafka中要先创建好topic
 * [root@hdp01 kafka_2.11-2.0.0]# bin/kafka-topics.sh --create --topic yinew_applog --partitions 2 --replication-factor 1 --zookeeper hdp01:2181,hdp02:2181,hdp03:2181
 *
 * 创建完后，检查一下是否创建成功：
 * [root@hdp01 kafka_2.11-2.0.0]# bin/kafka-topics.sh --list --zookeeper hdp01:2181
 *
 */
public class ActionLogGen {
    public static void main(String[] args) throws InterruptedException {


        // 创建多个线程，并行执行
        for(int i=0;i<10;i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {

                    Properties props = new Properties();
                    props.setProperty("bootstrap.servers", "hdp01:9092,hdp02:9092,hdp03:9092");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    // 构造一个kafka生产者客户端
                    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

                    while (true) {
                        LogBean logBean = new LogBean();
                        // 生成的账号形如： 004078
                        String account = StringUtils.leftPad(RandomUtils.nextInt(1, 100) + "", 6, "0");
                        logBean.setAccount(account);
                        logBean.setAppId("cn.doitedu.yinew");
                        logBean.setAppVersion("2.5");
                        logBean.setCarrier("中国移动");
                        // deviceid直接用account
                        logBean.setDeviceId(account);
                        logBean.setIp("10.102.36.88");
                        logBean.setLatitude(RandomUtils.nextDouble(10.0, 52.0));
                        logBean.setLongitude(RandomUtils.nextDouble(120.0, 160.0));
                        logBean.setDeviceType("mi6");
                        logBean.setNetType("5G");
                        logBean.setOsName("android");
                        logBean.setOsVersion("7.5");
                        logBean.setReleaseChannel("小米应用市场");
                        logBean.setResolution("2048*1024");


                        logBean.setEventId(RandomStringUtils.randomAlphabetic(1).toUpperCase());

                        HashMap<String, String> properties = new HashMap<String, String>();
                        for (int i = 0; i < RandomUtils.nextInt(1, 5); i++) {
                            // 生成的属性形如：  p1=v3, p2=v5, p3=v3,......
                            properties.put("p" + RandomUtils.nextInt(1, 11), "v" + RandomUtils.nextInt(1, 3));
                        }

                        logBean.setProperties(properties);
                        logBean.setTimeStamp(System.currentTimeMillis());
                        logBean.setSessionId(RandomStringUtils.randomNumeric(10, 10));


                        // 将日志对象，转成JSON
                        String log = JSON.toJSONString(logBean);
                        // 打印在控制台
                        // System.out.println(log);
                        // 写入kafka的topic： yinew_applog

                        ProducerRecord<String, String> record = new ProducerRecord<>("yinew_applog", log);
                        kafkaProducer.send(record);

                        try {
                            Thread.sleep(RandomUtils.nextInt(200, 301));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }


            }).start();


        }
    }
}

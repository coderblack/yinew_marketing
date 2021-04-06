package cn.doitedu.dynamic_rule.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-04-06
 * @desc 测试准备：
 * kafka中创建applicant topic
 * [root@hdp03 kafka_2.11-2.0.0]# bin/kafka-topics.sh --create --topic applicant --replication-factor 1 --partitions 1 --zookeeper hdp01:2181
 * [root@hdp03 kafka_2.11-2.0.0]# bin/kafka-topics.sh --create --topic test_drools --replication-factor 1 --partitions 1 --zookeeper hdp01:2181
 *
 *
 *
 */
@Slf4j
public class FlinkDroolsCanalDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 读申请信息流
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hdp01:9092,hdp02:9092,hdp03:9092");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> applicantConsumer = new FlinkKafkaConsumer<>("applicant", new SimpleStringSchema(), props);

        // joke,18
        DataStreamSource<String> applicantStrStream = env.addSource(applicantConsumer);

        // 将string流转成Applicant对象流
        SingleOutputStreamOperator<Applicant> applicantStream = applicantStrStream.map(new MapFunction<String, Applicant>() {
            @Override
            public Applicant map(String value) throws Exception {
                String[] split = value.split(",");
                return new Applicant(split[0],Integer.parseInt(split[1]));
            }
        });

        // 按照name来keyby
        KeyedStream<Applicant, String> keyedStream = applicantStream.keyBy(new KeySelector<Applicant, String>() {
            @Override
            public String getKey(Applicant value) throws Exception {
                return value.getName();
            }
        });


        // 读规则流，并广播
        FlinkKafkaConsumer<String> ruleConsumer = new FlinkKafkaConsumer<>("test_drools", new SimpleStringSchema(), props);
        DataStreamSource<String> ruleStream = env.addSource(ruleConsumer);

        MapStateDescriptor<String, KieSession> stateDescriptor = new MapStateDescriptor<>("ruleState", String.class, KieSession.class);
        BroadcastStream<String> broadcastStream = ruleStream.broadcast(stateDescriptor);

        // connect两个流
        BroadcastConnectedStream<Applicant, String> connectedStream = keyedStream.connect(broadcastStream);


        // 处理
        SingleOutputStreamOperator<String> result = connectedStream.process(new KeyedBroadcastProcessFunction<String, Applicant, String, String>() {

            BroadcastState<String, KieSession> broadcastState;

            /**
             * 处理数据流
             * @param applicant
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(Applicant applicant, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                Iterator<Map.Entry<String, KieSession>> rulesIterator = broadcastState.iterator();
                while(rulesIterator.hasNext()){
                    Map.Entry<String, KieSession> entry = rulesIterator.next();
                    KieSession kieSession = entry.getValue();

                    applicant.setValid(true);
                    kieSession.insert(applicant);
                    kieSession.fireAllRules();

                    if(applicant.isValid()){
                        out.collect(applicant.getName()+",合法");
                    }else{
                        out.collect(applicant.getName()+",不合法");
                    }

                }
            }

            /**
             * 处理广播流中的数据
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                broadcastState = ctx.getBroadcastState(stateDescriptor);

                // 进来的规则信息，是canal从mysql中监听到一个json串
                CanalRecordBean canalRecordBean = JSON.parseObject(value, CanalRecordBean.class);

                // 取出规则表数据
                RuleTableRecord tableRec = canalRecordBean.getData().get(0);

                String ruleName = tableRec.getRuleName();
                String ruleCode = tableRec.getRuleCode();

                KieSession kieSession = new KieHelper().addContent(ruleCode, ResourceType.DRL).build().newKieSession();

                broadcastState.put(ruleName,kieSession);

            }
        });

        // 打印
        result.print();

        env.execute();


    }
}

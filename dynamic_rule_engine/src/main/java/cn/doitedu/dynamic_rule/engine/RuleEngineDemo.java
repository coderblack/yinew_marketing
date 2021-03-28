package cn.doitedu.dynamic_rule.engine;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.ResultBean;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-27
 * @desc 实时运营系统版本1.0
 * <p>
 * 需求中要实现的判断规则：
 * 触发条件：E事件
 * 画像属性条件：  k3=v3 , k100=v80 , k230=v360
 * 行为属性条件：  U(p1=v3,p2=v2) >= 3次 且  G(p6=v8,p4=v5,p1=v2)>=1
 * 行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F
 */
public class RuleEngineDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);


        // 创建一个kafka数据源source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hdp01:9092,hdp02:9092,hdp03:9092");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("yinew_applog", new SimpleStringSchema(), props);

        // 将数据源添加到env中
        DataStreamSource<String> logStream = env.addSource(kafkaSource);

        // 将json串数据，转成bean对象数据
        SingleOutputStreamOperator<LogBean> beanStream = logStream.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String jsonLog) throws Exception {
                return JSON.parseObject(jsonLog, LogBean.class);
            }
        });


        // 对数据按用户keyby
        KeyedStream<LogBean, String> keyed = beanStream.keyBy(new KeySelector<LogBean, String>() {
            @Override
            public String getKey(LogBean bean) throws Exception {

                return bean.getDeviceId();
            }
        });

        // 在这个keyby后的数据流上，做规则判断
        SingleOutputStreamOperator<ResultBean> resultStream = keyed.process(new KeyedProcessFunction<String, LogBean, ResultBean>() {

            Connection conn;
            Table table;
            ListState<LogBean> eventState;


            @Override
            public void open(Configuration parameters) throws Exception {

                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("hbase.zookeeper.quorum", "hdp01:2181,hdp02:2181,hdp03:2181");

                conn = ConnectionFactory.createConnection(conf);
                table = conn.getTable(TableName.valueOf("yinew_profile"));

                // 定义一个list结构的state
                ListStateDescriptor<LogBean> eventStateDesc = new ListStateDescriptor<>("events_state", LogBean.class);
                eventState = getRuntimeContext().getListState(eventStateDesc);


            }

            @Override
            public void processElement(LogBean logBean, Context ctx, Collector<ResultBean> out) throws Exception {

                // 先将拿到的这条数据，存入state中攒起来
                eventState.add(logBean);

                // 判断当前的用户行为是否满足规则中的触发条件
                // 触发条件：E事件
                if ("E".equals(logBean.getEventId())) {
                    // 判断画像属性条件：  k3=v3 , k100=v80 , k230=v360
                    // 查询hbase即可

                    // 构造查询条件
                    Get get = new Get(Bytes.toBytes(logBean.getDeviceId()));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k3"));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k100"));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k230"));

                    // 传入查询条件并查询
                    Result result = table.get(get);
                    String k3_value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k3")));
                    String k100_value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k100")));
                    String k230_value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k230")));

                    // 如果画像属性条件全部满足
                    if ("v3".equals(k3_value) && "v80".equals(k100_value) && "v360".equals(k230_value)) {

                        // 则继续判断行为次数条件：  U(p1=v3,p2=v2) >= 3次 且  G(p6=v8,p4=v5,p1=v2)>=1


                        // 从state中捞出这个人历史以来的所有行为事件
                        Iterable<LogBean> logBeans = eventState.get();


                        int u_cnt = 0;
                        int g_cnt = 0;
                        for (LogBean bean : logBeans) {
                            // 计算U事件原子条件的次数
                            if (bean.getEventId().equals("U")) {
                                Map<String, String> properties = bean.getProperties();
                                String p1_value = properties.get("p1");
                                String p2_value = properties.get("p2");
                                if ("v3".equals(p1_value) && "v2".equals(p2_value)) u_cnt++;
                            }


                            // 计算G事件原子条件的次数
                            if (bean.getEventId().equals("G")) {
                                Map<String, String> properties = bean.getProperties();
                                String p6 = properties.get("p6");
                                String p4 = properties.get("p4");
                                String p1 = properties.get("p1");
                                if ("v8".equals(p6) && "v5".equals(p4) && "v2".equals(p1)) g_cnt++;
                            }

                        }

                        // 如果行为次数条件也满足
                        if (u_cnt >= 3 && g_cnt >= 1) {
                            ArrayList<LogBean> beanList = new ArrayList<>();
                            CollectionUtils.addAll(beanList, logBeans.iterator());
                            // 则，继续判断行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F

                            int index = -1;
                            for (int i = 0; i < beanList.size(); i++) {
                                LogBean beani = beanList.get(i);
                                if ("W".equals(beani.getEventId())) {
                                    Map<String, String> properties = beani.getProperties();
                                    String p1 = properties.get("p1");
                                    if ("v4".equals(p1)) {
                                        index = i;
                                        break;
                                    }
                                }
                            }

                            int index2 = -1;
                            if (index >= 0 && index + 1 < beanList.size()) {

                                for (int i = index + 1; i < beanList.size(); i++) {
                                    LogBean beani = beanList.get(i);
                                    if ("R".equals(beani.getEventId())) {
                                        Map<String, String> properties = beani.getProperties();
                                        String p2 = properties.get("p2");
                                        if ("v3".equals(p2)) {
                                            index2 = i;
                                            break;
                                        }
                                    }
                                }
                            }

                            int index3 = -1;
                            if (index2 >= 0 && index2 + 1 < beanList.size()) {

                                for (int i = index2 + 1; i < beanList.size(); i++) {
                                    LogBean beani = beanList.get(i);
                                    if ("F".equals(beani.getEventId())) {
                                        index3 = i;
                                        break;
                                    }
                                }
                            }

                            if(index3>-1){
                                ResultBean resultBean = new ResultBean();
                                resultBean.setDeviceId(logBean.getDeviceId());
                                resultBean.setRuleId("test_rule_1");
                                resultBean.setTimeStamp(logBean.getTimeStamp());
                                out.collect(resultBean);
                            }
                        }
                    }
                }
            }
        });


        resultStream.print();
        env.execute();

    }
}

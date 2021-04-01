package cn.doitedu.dynamic_rule.functions;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.ResultBean;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.service.QueryRouterV3;
import cn.doitedu.dynamic_rule.service.QueryRouterV4;
import cn.doitedu.dynamic_rule.utils.RuleSimulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 规则核心处理函数版本4.0
 */
public class RuleProcessFunctionV4 extends KeyedProcessFunction<String, LogBean, ResultBean> {

    QueryRouterV4 queryRouterV4;

    ListState<LogBean> eventState;

    RuleParam ruleParam;

    @Override
    public void open(Configuration parameters) throws Exception {

        /**
         * 获取规则参数
         * TODO 规则的获取，现在是通过模拟器生成
         * TODO 后期需要改造成从外部获取
         */
        ruleParam = RuleSimulator.getRuleParam();

        /**
         * 准备一个存储明细事件的state
         * 控制state的ttl周期为最近2小时
         */
        ListStateDescriptor<LogBean> desc = new ListStateDescriptor<>("eventState", LogBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).updateTtlOnCreateAndWrite().build();
        desc.enableTimeToLive(ttlConfig);
        eventState = getRuntimeContext().getListState(desc);

        // 构造一个查询路由控制器
        queryRouterV4 = new QueryRouterV4(eventState);

    }


    /**
     * 规则计算核心方法
     * @param logBean
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(LogBean logBean, Context ctx, Collector<ResultBean> out) throws Exception {

        // 将收到的事件放入历史明细state存储中
        // 超过2小时的logBean会被自动清除（前面设置了ttl存活时长）
        eventState.add(logBean);

        /**
         * 主逻辑，进行规则触发和计算
         */
        if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
            System.out.println("规则计算被触发：" + logBean.getDeviceId() + ","+logBean.getEventId());

            boolean b1 = queryRouterV4.profileQuery(logBean, ruleParam);
            if(!b1) return;

            boolean b2 = queryRouterV4.sequenceConditionQuery(logBean, ruleParam);
            if(!b2) return;

            boolean b3 = queryRouterV4.countConditionQuery(logBean, ruleParam);
            if(!b3) return;


            // 输出一个规则匹配成功的结果
            ResultBean resultBean = new ResultBean();
            resultBean.setTimeStamp(logBean.getTimeStamp());
            resultBean.setRuleId(ruleParam.getRuleId());
            resultBean.setDeviceId(logBean.getDeviceId());

            out.collect(resultBean);
        }
    }
}

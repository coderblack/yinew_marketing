package cn.doitedu.dynamic_rule.functions;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.ResultBean;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.service.QueryRouterV4;
import cn.doitedu.dynamic_rule.utils.RuleSimulator;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class RuleProcessFunctionV4 extends KeyedProcessFunction<String, LogBean, ResultBean> {

    QueryRouterV4 queryRouterV4;

    ListState<LogBean> eventState;

    // RuleParam ruleParam;

    @Override
    public void open(Configuration parameters) throws Exception {

        /*
         * 获取规则参数
         * TODO 规则的获取，现在是通过模拟器生成
         * TODO 后期需要改造成从外部获取
         */
        // ruleParam = RuleSimulator.getRuleParam();

        /*
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
     * @param logBean 事件bean
     * @param ctx 上下文
     * @param out 输出
     * @throws Exception 异常
     */
    @Override
    public void processElement(LogBean logBean, Context ctx, Collector<ResultBean> out) throws Exception {

        // 将收到的事件放入历史明细state存储中
        // 超过2小时的logBean会被自动清除（前面设置了ttl存活时长）
        eventState.add(logBean);

        // TODO 重大bug，下面的处理过程会不断修改ruleParam中条件的时间字段，如果不在这里重新获取规则，则下一次触发计算时，条件已经不是原来的条件了
        RuleParam ruleParam = RuleSimulator.getRuleParam();


        /*
         * 主逻辑，进行规则触发和计算
         */
        if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
            log.debug("规则:{},用户:{},触发事件:{},触发时间:{}", ruleParam.getRuleId(),logBean.getDeviceId(),logBean.getEventId(),logBean.getTimeStamp());

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
            log.info("{}规则,触发人:{},计算匹配成功", ruleParam.getRuleId(),logBean.getDeviceId());

            out.collect(resultBean);
        }
    }
}

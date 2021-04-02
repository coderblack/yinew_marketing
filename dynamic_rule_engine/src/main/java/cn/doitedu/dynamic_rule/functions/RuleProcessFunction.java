package cn.doitedu.dynamic_rule.functions;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.ResultBean;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.service.*;
import cn.doitedu.dynamic_rule.utils.RuleSimulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 规则核心处理函数
 */
public class RuleProcessFunction extends KeyedProcessFunction<String, LogBean, ResultBean> {

    private UserProfileQueryService userProfileQueryService;
    private UserActionCountQueryService userActionCountQueryService;
    private UserActionSequenceQueryService userActionSequenceQueryService;

    ListState<LogBean> eventState;

    RuleParam ruleParam;

    @Override
    public void open(Configuration parameters) throws Exception {

        /**
         * 准备一个存储明细事件的state
         */
        ListStateDescriptor<LogBean> desc = new ListStateDescriptor<>("eventState", LogBean.class);
        eventState = getRuntimeContext().getListState(desc);
        /**
         * 构造底层的核心查询服务
         */
        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();
        userActionCountQueryService = new UserActionCountQueryServiceStateImpl(eventState);
        userActionSequenceQueryService = new UserActionSequenceQueryServiceStateImpl();

        /**
         * 获取规则参数
         */
        ruleParam = RuleSimulator.getRuleParam();



    }

    @Override
    public void processElement(LogBean logBean, Context ctx, Collector<ResultBean> out) throws Exception {
        // 将收到的事件放入历史明细state存储中
        eventState.add(logBean);


        // 判断是否满足触发条件
        if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
            System.out.println("规则计算被触发：" + logBean.getDeviceId() + ","+logBean.getEventId());

            // 查询画像条件
            boolean profileMatch = userProfileQueryService.judgeProfileCondition(logBean.getDeviceId(), ruleParam);
            if(!profileMatch) return;

            // 查询行为次数条件
            boolean countMatch = userActionCountQueryService.queryActionCounts("", ruleParam);
            if(!countMatch) return;

            // 查询行为序列条件
            boolean sequenceMatch = userActionSequenceQueryService.queryActionSequence(null,eventState, ruleParam);
            if(!sequenceMatch) return;


            // 输出一个规则匹配成功的结果
            ResultBean resultBean = new ResultBean();
            resultBean.setTimeStamp(logBean.getTimeStamp());
            resultBean.setRuleId(ruleParam.getRuleId());
            resultBean.setDeviceId(logBean.getDeviceId());

            out.collect(resultBean);
        }
    }
}

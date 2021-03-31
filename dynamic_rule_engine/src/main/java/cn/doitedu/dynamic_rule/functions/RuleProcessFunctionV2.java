package cn.doitedu.dynamic_rule.functions;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.ResultBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.service.*;
import cn.doitedu.dynamic_rule.utils.RuleSimulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 规则核心处理函数
 */
public class RuleProcessFunctionV2 extends KeyedProcessFunction<String, LogBean, ResultBean> {

    private UserProfileQueryService userProfileQueryService;

    private UserActionCountQueryService userActionCountQueryStateService;
    private UserActionSequenceQueryService userActionSequenceQueryStateService;

    private UserActionCountQueryService userActionCountQueryClickhouseService;
    private UserActionSequenceQueryService userActionSequenceQueryClickhouseService;

    ListState<LogBean> eventState;

    RuleParam ruleParam;

    @Override
    public void open(Configuration parameters) throws Exception {

        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();

        /**
         * 构造底层的核心STATE查询服务
         */
        userActionCountQueryStateService = new UserActionCountQueryServiceStateImpl();
        userActionSequenceQueryStateService = new UserActionSequenceQueryServiceStateImpl();

        /**
         * 构造底层的核心CLICKHOUSE查询服务
         */
        userActionCountQueryClickhouseService = new UserActionCountQueryServiceClickhouseImpl();
        userActionSequenceQueryClickhouseService = new UserActionSequenceQueryServiceClickhouseImpl();


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

        // 计算当前时间的前2小时，时间戳
        long splitPoint = System.currentTimeMillis() - 2*60*60*1000;

        /**
         * 主逻辑，进行规则触发和计算
         */
        if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
            System.out.println("规则计算被触发：" + logBean.getDeviceId() + ","+logBean.getEventId());

            // 查询画像条件
            boolean profileIfMatch = userProfileQueryService.judgeProfileCondition(logBean.getDeviceId(), ruleParam);
            if(!profileIfMatch) return;


            /**
             * 查询事件count类条件是否满足
             * 这里要考虑，条件的时间跨度问题
             * 如果条件的时间跨度在近2小时内，那么，就把这些条件交给stateService去计算
             * 如果条件的时间跨度在2小时之前，那么，就把这些条件交给clickhouseService去计算
             */
            // 遍历规则中的事件次数类条件，按照时间跨度，分成两组
            ArrayList<RuleAtomicParam> farRangeParams = new ArrayList<>();  // 存远跨度条件的list
            ArrayList<RuleAtomicParam> nearRangeParams = new ArrayList<>();  // 存近跨度条件的list

            List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();
            for (RuleAtomicParam userActionCountParam : userActionCountParams) {
                if(userActionCountParam.getRangeStart()<splitPoint){
                    // 如果条件起始时间<2小时分界点，放入远期条件租
                    farRangeParams.add(userActionCountParam);
                }else{
                    // 如果条件起始时间>=2小时分界点，放入近期条件组
                    nearRangeParams.add(userActionCountParam);
                }
            }

            // 在state中查询行为次数条件
            if(nearRangeParams.size()>0) {
                // 将规则总参数对象中的“次数类条件”覆盖成： 近期条件组
                ruleParam.setUserActionCountParams(nearRangeParams);
                // 交给stateService对这一组条件进行计算
                boolean countMatch = userActionCountQueryStateService.queryActionCounts("", eventState, ruleParam);
                if (!countMatch) return;
            }

            // 如果在state中查询的条件组都满足，则继续在clickhouse中查询远期条件组
            if(farRangeParams.size()>0) {
                // 将规则总参数对象中的“次数类条件”覆盖成： 远期条件组
                ruleParam.setUserActionCountParams(farRangeParams);
                boolean b = userActionCountQueryClickhouseService.queryActionCounts(logBean.getDeviceId(), null, ruleParam);
                if (!b) return;
            }


            /**
             * sequence条件的查询
             * 本项目对序列类条件进行了简化
             * 一个规则中，只存在一个“序列模式”
             * 它的条件时间跨度，设置在了这个“序列模式”中的每一个原子条件中，且都一致
             */
            // 取出规则中的“序列模式”
            List<RuleAtomicParam> userActionSequenceParams = ruleParam.getUserActionSequenceParams();

            // 如果序列模型中的起始时间>=2小时分界点，则交给state服务模块去查询
            if(userActionSequenceParams.size()>0 && userActionSequenceParams.get(0).getRangeStart()>=splitPoint){
                boolean b = userActionSequenceQueryStateService.queryActionSequence("", eventState, ruleParam);
                if(!b) return ;
            }


            // 如果序列模型中的起始时间<2小时分界点，则交给clickhouse服务模块去查询
            if(userActionSequenceParams!=null && userActionSequenceParams.size()>0 && userActionSequenceParams.get(0).getRangeStart()<splitPoint){
                boolean b = userActionSequenceQueryClickhouseService.queryActionSequence(logBean.getDeviceId(), null, ruleParam);
                if(!b) return ;
            }


            // 输出一个规则匹配成功的结果
            ResultBean resultBean = new ResultBean();
            resultBean.setTimeStamp(logBean.getTimeStamp());
            resultBean.setRuleId(ruleParam.getRuleId());
            resultBean.setDeviceId(logBean.getDeviceId());

            out.collect(resultBean);
        }
    }
}

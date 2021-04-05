package cn.doitedu.dynamic_rule.service;


import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.utils.RuleCalcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.ListState;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 用户行为次序类条件查询服务实现（在state中查询）
 */
@Slf4j
public class UserActionSequenceQueryServiceStateImpl implements UserActionSequenceQueryService {

    //  eventState flink中存储用户事件明细的state
    ListState<LogBean> eventState;

    public UserActionSequenceQueryServiceStateImpl(ListState<LogBean> eventState){
        this.eventState = eventState;
    }



    /**
     * 行为序列： A(p1=v2,p3=v8) B(p6=v9,p7=v7)  C()
     * 意味着用户要依顺序做过上面的事件
     * <p>
     * 查询规则条件中的行为序列条件是否满足
     * 会将查询到的最大匹配步骤，set回 ruleParam对象中
     * @param ruleParam  规则参数对象
     * @return 条件成立与否
     */
    @Override
    public boolean queryActionSequence(String deviceId,RuleParam ruleParam) throws Exception {


        Iterable<LogBean> logBeans = eventState.get();
        List<RuleAtomicParam> userActionSequenceParams = ruleParam.getUserActionSequenceParams();

        // 调用helper统计实际匹配的最大步骤号
        int maxStep = queryActionSequenceHelper2(logBeans, userActionSequenceParams);

        // 将这个maxStep丢回规则参数对象，以便本服务的调用者可以根据需要获取到这个最大匹配步骤号
        ruleParam.setUserActionSequenceQueriedMaxStep(ruleParam.getUserActionSequenceQueriedMaxStep()+maxStep);

        // 然后判断整个序列条件是否满足：真实最大匹配步骤是否等于条件的步骤数
        log.debug("序列匹配:state,规则:{},用户:{},结果maxStep：{},条件步数:{}, ",ruleParam.getRuleId(),deviceId,maxStep,userActionSequenceParams.size());
        return maxStep == userActionSequenceParams.size();
    }


    /**
     * 统计明细事件中，与序列条件匹配到的最大步骤
     *
     * @param events 事件明细
     * @param userActionSequenceParams 行为序列规则条件
     * @return 行为次数
     */
    public int queryActionSequenceHelper(Iterable<LogBean> events, List<RuleAtomicParam> userActionSequenceParams) {

        ArrayList<LogBean> eventList = new ArrayList<>();
        CollectionUtils.addAll(eventList, events.iterator());

        // 外循环，遍历每一个条件
        int maxStep = 0;
        int index = 0;
        for (RuleAtomicParam userActionSequenceParam : userActionSequenceParams) {

            // 内循环，遍历每一个历史明细事件，看看能否找到与当前条件匹配的事件
            boolean isFind = false;
            for (int i = index; i < eventList.size(); i++) {
                LogBean logBean = eventList.get(i);
                // 判断当前的这个事件 logBean，是否满足当前规则条件 userActionSequenceParam
                boolean match = RuleCalcUtil.eventBeanMatchEventParam(logBean, userActionSequenceParam, true);
                // 如果匹配，则最大步骤号+1，且更新下一次内循环的起始位置,并跳出本轮内循环
                if (match) {
                    maxStep++;
                    index = i + 1;
                    isFind = true;
                    break;
                }
            }

            if (!isFind) break;

        }

        log.debug("在state中步骤匹配计算完成： 查询到的最大步骤号为： " + maxStep + ",条件中的步骤数为：" + userActionSequenceParams.size());
        return maxStep;
    }

    /**
     * 序列匹配，性能改进版
     * @param events 事件明细
     * @param userActionSequenceParams 行为序列规则条件
     * @return 次数
     */
    public int queryActionSequenceHelper2(Iterable<LogBean> events, List<RuleAtomicParam> userActionSequenceParams) {

        int maxStep = 0;
        for (LogBean event : events) {
            if (RuleCalcUtil.eventBeanMatchEventParam(event, userActionSequenceParams.get(maxStep))) {
                maxStep++;
            }
            if (maxStep == userActionSequenceParams.size()) break;
        }
        return maxStep;
    }

}

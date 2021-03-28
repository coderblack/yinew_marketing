package cn.doitedu.dynamic_rule.service;


import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 用户行为次序类条件查询服务实现（在state中查询）
 */
public class UserActionSequenceQueryServiceStateImpl implements UserActionSequenceQueryService {


    /**
     * 行为序列： A(p1=v2,p3=v8) B(p6=v9,p7=v7)  C()
     * 意味着用户要依顺序做过上面的事件
     *
     * 查询规则条件中的行为序列条件是否满足
     * 会将查询到的最大匹配步骤，set回 ruleParam对象中
     * @param eventState flink中存储用户事件明细的state
     * @param ruleParam  规则参数对象
     * @return 条件成立与否
     */
    @Override
    public boolean queryActionSequence(ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {


        Iterable<LogBean> logBeans = eventState.get();
        List<RuleAtomicParam> userActionSequenceParams = ruleParam.getUserActionSequenceParams();

        // 调用helper统计实际匹配的最大步骤号
        int maxStep = queryActionSequenceHelper(logBeans, userActionSequenceParams);

        // 将这个maxStep丢回规则参数对象，以便本服务的调用者可以根据需要获取到这个最大匹配步骤号
        ruleParam.setUserActionSequenceQueriedMaxStep(maxStep);


        // 然后判断整个序列条件是否满足：真实最大匹配步骤是否等于条件的步骤数
        return maxStep==userActionSequenceParams.size();
    }


    /**
     * 统计明细事件中，与序列条件匹配到的最大步骤
     * @param events
     * @param userActionSequenceParams
     * @return
     */
    public int queryActionSequenceHelper(Iterable<LogBean> events,List<RuleAtomicParam> userActionSequenceParams){









        return 0;
    }




}

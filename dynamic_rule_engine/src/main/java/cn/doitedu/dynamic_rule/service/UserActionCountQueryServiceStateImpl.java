package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.utils.RuleCalcUtil;
import org.apache.flink.api.common.state.ListState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 用户行为次数类条件查询服务实现：在flink的state中统计行为次数
 */
public class UserActionCountQueryServiceStateImpl implements UserActionCountQueryService {


    /**
     * 查询规则参数对象中，要求的用户行为次数类条件是否满足
     * 同时，将查询到的真实次数，set回 规则参数对象中
     *
     * @param eventState 用户事件明细存储state
     * @param ruleParam  规则整体参数对象
     * @return 条件是否满足
     */
    public boolean queryActionCounts(ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {

        // 取出各个用户行为次数原子条件
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        // 取出历史明细数据
        Iterable<LogBean> logBeansIterable = eventState.get();


        // 统计每一个原子条件所发生的真实次数，就在原子条件参数对象中：realCnts
        queryActionCountsHelper(logBeansIterable, userActionCountParams);


        // 经过上面的方法执行后，每一个原子条件中，都拥有了一个真实发生次数，我们在此判断是否每个原子条件都满足
        for (RuleAtomicParam userActionCountParam : userActionCountParams) {
            if (userActionCountParam.getRealCnts() < userActionCountParam.getCnts()) {
                return false;
            }
        }


        // 如果到达这一句话，说明上面的判断中，每个原子条件都满足，则返回整体结果true
        return true;
    }


    /**
     * 根据传入的历史明细，和规则条件
     * 挨个统计每一个规则原子条件的真实发生次数，并将结果set回规则条件参数中
     *
     * @param logBeansIterable
     * @param userActionCountParams
     */
    public void queryActionCountsHelper(Iterable<LogBean> logBeansIterable, List<RuleAtomicParam> userActionCountParams) {
        for (LogBean logBean : logBeansIterable) {

            for (RuleAtomicParam userActionCountParam : userActionCountParams) {

                // 判断当前logbean 和当前 规则原子条件userActionCountParam 是否一致
                boolean isMatch = RuleCalcUtil.eventBeanMatchEventParam(logBean, userActionCountParam,true);

                // 如果一致，则查询次数结果+1
                if (isMatch) {
                    userActionCountParam.setRealCnts(userActionCountParam.getRealCnts() + 1);
                }
            }

        }

    }


}

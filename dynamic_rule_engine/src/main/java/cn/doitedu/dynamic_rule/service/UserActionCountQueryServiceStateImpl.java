package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
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
     * @param eventState  用户事件明细存储state
     * @param ruleParam 规则整体参数对象
     * @return 条件是否满足
     */
    public boolean queryActionCounts(ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {

        // 取出用户行为次数类条件
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        // 迭代每一个历史明细事件
        Iterable<LogBean> logBeansIterable = eventState.get();


        boolean res = queryActionCountsHelper(logBeansIterable, userActionCountParams);

        return res;
    }





    public boolean queryActionCountsHelper(Iterable<LogBean> logBeansIterable,List<RuleAtomicParam> userActionCountParams){
        for (LogBean logBean : logBeansIterable) {

            for (RuleAtomicParam userActionCountParam : userActionCountParams) {

                // 判断当前logbean 和当前 规则原子条件userActionCountParam 是否一致

                // 如果一致，则查询次数结果+1

                // 否则，啥也不做

            }


        }

        return false;
    }


    /**
     * 工具方法，用于判断一个待判断事件和一个规则中的原子条件是否一致
     * @param eventBean
     * @param eventParam
     * @return
     */
    private boolean eventBeanMatchEventParam(LogBean eventBean,RuleAtomicParam eventParam){
        // 如果传入的一个事件的事件id与参数中的事件id相同，才开始进行属性判断
        if(eventBean.getEventId().equals(eventParam.getEventId())){

            // 取出待判断事件中的属性
            Map<String, String> eventProperties = eventBean.getProperties();

            // 取出条件中的事件属性
            HashMap<String, String> paramProperties = eventParam.getProperties();
            Set<Map.Entry<String, String>> entries = paramProperties.entrySet();
            // 遍历条件中的每个属性及值
            for (Map.Entry<String, String> entry : entries) {
                if(!entry.getValue().equals(eventProperties.get(entry.getKey()))){
                    return false;
                }
            }

            return true;
        }

        return false;
    }



}

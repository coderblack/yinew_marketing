package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

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
    public boolean queryActionCounts(ListState<LogBean> eventState, RuleParam ruleParam) {




        return false;
    }





}

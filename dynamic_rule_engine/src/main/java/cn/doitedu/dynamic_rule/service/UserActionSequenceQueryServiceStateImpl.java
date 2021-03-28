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
 * @desc 用户行为次序类条件查询服务实现（在state中查询）
 */
public class UserActionSequenceQueryServiceStateImpl implements UserActionSequenceQueryService {


    /**
     * 查询规则条件中的 行为序列条件
     * 会将查询到的最大匹配步骤，set回 ruleParam对象中
     * @param eventState flink中存储用户事件明细的state
     * @param ruleParam  规则参数对象
     * @return 条件成立与否
     */
    @Override
    public boolean queryActionSequence(ListState<LogBean> eventState, RuleParam ruleParam){



        return false;
    }




}

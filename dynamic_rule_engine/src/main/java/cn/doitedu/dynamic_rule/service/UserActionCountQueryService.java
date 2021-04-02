package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 用户行为次数类条件查询服务接口
 */
public interface UserActionCountQueryService {

    public boolean queryActionCounts(String deviceId, RuleParam ruleParam) throws Exception;

    public boolean queryActionCounts(String deviceId, RuleAtomicParam atomicParam) throws Exception;

}

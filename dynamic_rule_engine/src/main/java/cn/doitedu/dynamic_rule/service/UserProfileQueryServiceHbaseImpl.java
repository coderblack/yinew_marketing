package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.RuleParam;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 用户画像查询服务，hbase查询实现类
 */
public class UserProfileQueryServiceHbaseImpl implements UserProfileQueryService {


    /**
     * 传入一个用户号，以及要查询的条件
     * 返回这些条件是否满足
     * @param deviceId
     * @param ruleParam
     * @return
     */
    @Override
    public boolean judgeProfileCondition(String deviceId, RuleParam ruleParam){



        return false;
    }


}

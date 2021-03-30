package cn.doitedu.dynamic_rule.utils;

import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 规则模拟器
 */
public class RuleSimulator {

    public static RuleParam getRuleParam(){

        RuleParam ruleParam = new RuleParam();
        ruleParam.setRuleId("test_rule_1");

        // 构造触发条件
        RuleAtomicParam trigger = new RuleAtomicParam();
        trigger.setEventId("E");
        ruleParam.setTriggerParam(trigger);


        // 构造画像条件
        HashMap<String, String> userProfileParams = new HashMap<>();
        userProfileParams.put("tag42","v8");
        //userProfileParams.put("tag47","v53");
        ruleParam.setUserProfileParams(userProfileParams);


        // 行为次数条件
        RuleAtomicParam count1 = new RuleAtomicParam();
        count1.setEventId("B");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v1");
        count1.setProperties(paramProps1);
        count1.setRangeStart(0);
        count1.setRangeEnd(Long.MAX_VALUE);
        count1.setCnts(2);

        RuleAtomicParam count2 = new RuleAtomicParam();
        count2.setEventId("D");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2","v3");
        count2.setProperties(paramProps2);
        count2.setRangeStart(1617094800000L);
        count2.setRangeEnd(Long.MAX_VALUE);
        count2.setCnts(1);


        ArrayList<RuleAtomicParam> countParams = new ArrayList<>();
        countParams.add(count1);
        countParams.add(count2);
        ruleParam.setUserActionCountParams(countParams);


        // 行为序列（行为路径）条件(2个事件的序列）
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("A");
        HashMap<String, String> seqProps1 = new HashMap<>();
        seqProps1.put("p1","v1");
        param1.setProperties(seqProps1);
        param1.setRangeStart(0);
        param1.setRangeEnd(Long.MAX_VALUE);

        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("C");
        HashMap<String, String> seqProps2 = new HashMap<>();
        seqProps2.put("p2","v2");
        param2.setProperties(seqProps2);
        param2.setRangeStart(0);
        param2.setRangeEnd(Long.MAX_VALUE);


        ArrayList<RuleAtomicParam> ruleParams = new ArrayList<>();
        ruleParams.add(param1);
        ruleParams.add(param2);

        ruleParam.setUserActionSequenceParams(ruleParams);


        return  ruleParam;
    }
}

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
        userProfileParams.put("tag5","v5");
        //userProfileParams.put("tag6","v2");
        ruleParam.setUserProfileParams(userProfileParams);


        // 行为次数条件
        RuleAtomicParam count1 = new RuleAtomicParam();
        count1.setEventId("B");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v1");
        count1.setProperties(paramProps1);
        count1.setOriginStart(0);
        count1.setOriginEnd(Long.MAX_VALUE);
        count1.setCnt(4);
        String sql1 = "select\n" +
                "    deviceId,\n" +
                "    count(1) as cnt\n" +
                "from yinew_detail\n" +
                "where deviceId='${deviceid}' and eventId='B' and properties['p1']='v1'\n" +
                "  and timeStamp between 0 and 6615900580000\n" +
                "group by deviceId\n" +
                ";";
        count1.setCountQuerySql(sql1);

        RuleAtomicParam count2 = new RuleAtomicParam();
        count2.setEventId("D");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2","v2");
        count2.setProperties(paramProps2);
        count2.setOriginStart(1617094800000L);
        count2.setOriginEnd(Long.MAX_VALUE);
        count2.setCnt(1);
        String sql2 = "select\n" +
                "    deviceId,\n" +
                "    count(1) as cnt\n" +
                "from yinew_detail\n" +
                "where deviceId='${deviceid}' and eventId='D' and properties['p2']='v2'\n" +
                "  and timeStamp between 1617094800000 and 6615900580000\n" +
                "group by deviceId \n" +
                ";";
        count2.setCountQuerySql(sql2);


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
        param1.setOriginStart(0);
        param1.setOriginEnd(Long.MAX_VALUE);

        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("C");
        HashMap<String, String> seqProps2 = new HashMap<>();
        seqProps2.put("p2","v2");
        param2.setProperties(seqProps2);
        param2.setOriginStart(0);
        param2.setOriginEnd(Long.MAX_VALUE);


        ArrayList<RuleAtomicParam> ruleParams = new ArrayList<>();
        ruleParams.add(param1);
        ruleParams.add(param2);

        ruleParam.setUserActionSequenceParams(ruleParams);
        String sql = "SELECT\n" +
                "  deviceId,\n" +
                "  sequenceMatch('.*(?1).*(?2).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'A' and properties['p1']='v1',\n" +
                "    eventId = 'C' and properties['p2']='v2'\n" +
                "  ) as isMatch2,\n" +
                "\n" +
                "  sequenceMatch('.*(?1).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'A' and properties['p1']='v1',\n" +
                "    eventId = 'C' and properties['p2']='v2'\n" +
                "  ) as isMatch1\n" +
                "\n" +
                "from yinew_detail\n" +
                "where\n" +
                "  deviceId = '${deviceid}'\n" +
                "    and\n" +
                "  timeStamp >= 0\n" +
                "    and\n" +
                "  timeStamp <= 6235295739479\n" +
                "    and\n" +
                "  (\n" +
                "        (eventId='A' and properties['p1']='v1')\n" +
                "     or (eventId = 'C' and properties['p2']='v2')\n" +
                "  )\n" +
                "group by deviceId;";
        ruleParam.setActionSequenceQuerySql(sql);

        return  ruleParam;
    }
}

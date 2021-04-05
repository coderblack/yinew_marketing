package cn.doitedu.dynamic_rule.moduletest;

import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.service.UserActionCountQueryServiceClickhouseImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ActionCountsQueryClickhouseTest {

    public static void main(String[] args) throws Exception {

        UserActionCountQueryServiceClickhouseImpl impl = new UserActionCountQueryServiceClickhouseImpl();


        // 构造2个规则原子条件
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("B");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v7");
        param1.setRangeStart(0);
        param1.setRangeEnd(Long.MAX_VALUE);
        param1.setProperties(paramProps1);
        param1.setCnt(2);

        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("W");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2","v3");
        param2.setProperties(paramProps2);
        param2.setRangeStart(0);
        param2.setRangeEnd(Long.MAX_VALUE);
        param2.setCnt(2);

        ArrayList<RuleAtomicParam> ruleParams = new ArrayList<>();
        ruleParams.add(param1);
        ruleParams.add(param2);

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserActionCountParams(ruleParams);


        boolean b = impl.queryActionCounts("000001",  ruleParam);
        List<RuleAtomicParam> params = ruleParam.getUserActionCountParams();
        for (RuleAtomicParam param : params) {
            System.out.println(param.getCnt() + ", " + param.getRealCnt());
        }


        System.out.println(b);



    }
}

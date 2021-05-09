package cn.doitedu.yinew.manageplatform.controller;


import cn.doitedu.yinew.manageplatform.pojo.RuleStatus;
import com.alibaba.fastjson.JSON;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
public class RuleDashboardController {
    Jedis jedis = null;
    public RuleDashboardController(){
        jedis = new Jedis("hdp02", 6379);
    }

    /**
     * 获取所有规则的状态信息
     *
     * {
     *   ruleName: '运营公众号拉新',
     *   ruleType: '触发型',
     *   publishTime: '2021-04-01 12:30:45',
     *   lastTrigTime: '2021-06-08 13:30:30',
     *   trigCount: 80,
     *   hitCount: 20,
     *   hitRatio: '30%',
     *   compareGroupRatio: '20%',
     *   ruleGroupRation: '40%',
     *   runStatus:"1"
     * }
     *
     * @return
     */
    @RequestMapping(method = RequestMethod.POST,value = "/api/getrulestatus")
    @CrossOrigin(origins = "http://localhost:8000")
    public List<RuleStatus> getRuleStatus(@RequestBody String a){

        Set<String> ruleKeys = jedis.keys("rule_status_*");
        ArrayList<RuleStatus> lst = new ArrayList<>();
        for (String ruleKey : ruleKeys) {
            Map<String, String> stringStringMap = jedis.hgetAll(ruleKey);
            String json = JSON.toJSONString(stringStringMap);
            RuleStatus ruleStatus = JSON.parseObject(json, RuleStatus.class);
            lst.add(ruleStatus);
        }


         /*
         ArrayList<RuleStatus> lst = new ArrayList<RuleStatus>();
         RuleStatus r1 = new RuleStatus("运营公众号拉新", "001", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "1");
         RuleStatus r2 = new RuleStatus("3H爆款激活用户", "002", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "1");
         RuleStatus r3 = new RuleStatus("拉新促销", "003", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "0");
         RuleStatus r4 = new RuleStatus("优惠关键词", "004", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "1");
         RuleStatus r5 = new RuleStatus("高流失风险客户挽留", "005", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "1");
         RuleStatus r6 = new RuleStatus("新客激活优惠券发送", "006", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "0");
         RuleStatus r7 = new RuleStatus("双11爆款硬推", "007", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "1");
         RuleStatus r8 = new RuleStatus("夏日饮品新上架普推", "008", "2021-06-10 12:30:30", "2021-06-10 12:30:30", "触发型", "800", "40%", "200", "20%", "35%", "1");
        lst.add(r1);
        lst.add(r2);
        lst.add(r3);
        lst.add(r4);
        lst.add(r5);
        lst.add(r6);
        lst.add(r7);
        lst.add(r8);*/

        return lst;
    }




}

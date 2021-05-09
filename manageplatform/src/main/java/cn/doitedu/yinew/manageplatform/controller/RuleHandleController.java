package cn.doitedu.yinew.manageplatform.controller;

import cn.doitedu.yinew.manageplatform.pojo.RuleDefine;
import cn.doitedu.yinew.manageplatform.pojo.RuleStatus;
import com.alibaba.fastjson.JSON;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.*;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2021/5/9
 *
 * 仅为演示代码
 * 后端管理平台，开发工作量很大，会javaweb开发的同学看到演示代码即可知真实开发思路
 **/
@RestController
public class RuleHandleController {

    Jedis jedis = null;
    public RuleHandleController(){
        jedis = new Jedis("hdp02", 6379);
    }


    @RequestMapping(method = RequestMethod.POST, value = "/api/publishrule")
    @CrossOrigin(origins = "http://localhost:8000")
    public String publishRule(@RequestBody RuleDefine ruleDefine) {

        // 接收到规则定义信息后

        // 利用freemarker或者velocity，生成规则所需的sql模板，和drools模板文件

        // 将规则定义信息及生成好的sql、drools代码，插入mysql数据库，以供canal监听并通过kafka传递给flink规则引擎

        // 此处纯为演示，直接将规则信息写入“监控平台”所要读取的redis库
        HashMap<String, String> ruleStatus = new HashMap<>();
        /*
            private String ruleName;
            private String ruleId;
            private String lastTrigTime;
            private String publishTime;
            private String ruleType;
            private long trigCount;
            private String hitRatio;
            private long hitCount;
            private String compareGroupRatio;
            private String ruleGroupRatio;
            private boolean runStatus;
         */

        Long ruleId = jedis.incr("rule_num");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String publishTime = sdf.format(new Date());
        String ruleType = new Random().nextInt(10)%3 ==0 ?"触发型":"单次型";

        ruleStatus.put("ruleName", ruleDefine.getRuleName());
        RuleStatus r1 = new RuleStatus(ruleDefine.getRuleName(), ruleId + "", "", publishTime, ruleType, "0", "0%", "0", "0%", "0%", "1");
        String s = JSON.toJSONString(r1);
        HashMap<String, String> map = (HashMap<String, String>) JSON.parseObject(s, Map.class);
        System.out.println(map);
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            jedis.hset("rule_status_" + ruleId, entry.getKey(),entry.getValue());
        }


        return "ok";
    }
}

package cn.doitedu.dynamic_rule.utils;

import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-30
 * @desc 行为次数类条件查询sql拼接工具
 */
public class ClickhouseCountQuerySqlUtil {

    public static String getSql(String deviceId,RuleAtomicParam atomicParam){

        String templet1 =
                "select " +
                "\n deviceId,count() as cnt " +
                "\n from yinew_detail " +
                "\n where deviceId= '" +deviceId+"'" +
                "\n and " +
                "\n eventId = '" + atomicParam.getEventId()+"' " +
                "\n and " +
                "\n timeStamp >= "+atomicParam.getRangeStart()+" and timeStamp <=" + atomicParam.getRangeEnd();


        String templet3 = "\n group by deviceId";


        HashMap<String, String> properties = atomicParam.getProperties();
        Set<Map.Entry<String, String>> entries = properties.entrySet();


        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, String> entry : entries) {
            // "and properties['pageId'] = 'page006'"
            sb.append("\n and properties['"+entry.getKey()+"']='"+entry.getValue()+"'");

        }

        return templet1+sb.toString()+ templet3;
    }

    public static void main(String[] args) {

        RuleAtomicParam ruleAtomicParam = new RuleAtomicParam();
        ruleAtomicParam.setEventId("R");
        HashMap<String, String> props = new HashMap<>();
        props.put("p1","v2");
        props.put("p3","v4");
        props.put("p7","v5");
        ruleAtomicParam.setProperties(props);
        ruleAtomicParam.setRangeStart(1616925000815L);
        ruleAtomicParam.setRangeEnd(1616925004815L);

        String sql = getSql("000001", ruleAtomicParam);
        System.out.println(sql);


    }

}

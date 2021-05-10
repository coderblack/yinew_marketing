package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.utils.ConnectionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-30
 * @desc 行为序列类路径匹配查询service，clickhouse实现
 */
@Slf4j
public class UserActionSequenceQueryServiceClickhouseImpl implements UserActionSequenceQueryService{

    private static Connection conn;


   static {
       try {
           conn = ConnectionUtils.getClickhouseConnection();
       } catch (Exception e) {
           e.printStackTrace();
       }
   }

    /**
     * 从clickhouse中查询行为序列条件是否满足
     *
     * @param deviceId
     * @param ruleParam
     * @return
     * @throws Exception
     */
    @Override
    public boolean queryActionSequence(String deviceId, RuleParam ruleParam) throws Exception {
        // 获取规则中，路径模式的总步骤数
        int totalStep = ruleParam.getUserActionSequenceParams().size();
        RuleAtomicParam ruleAtomicParam = ruleParam.getUserActionSequenceParams().get(0);

        // 取出查询sql
        String sql = ruleParam.getActionSequenceQuerySql();
        PreparedStatement ps = conn.prepareStatement(sql);
        // 需要将sql中的deviceId占位符替换成真实deviceId
        ps.setString(1,deviceId);
        ps.setLong(2,ruleAtomicParam.getRangeStart());
        ps.setLong(3,ruleAtomicParam.getRangeEnd());
        ResultSet resultSet = ps.executeQuery();

        /*
          sql = sql.replaceAll("\\$\\{did\\}",deviceId);
          Statement statement = conn.createStatement();
          ResultSet resultSet = statement.executeQuery(sql);
        */

        // 执行查询
        long s = System.currentTimeMillis();



        /* 从返回结果中进行条件判断
         * ┌─deviceId─┬─isMatch3─┬─isMatch2─┬─isMatch1─┐
         * │ 000001   │       0  │        0 │        1 │
         * └──────────┴──────────┴──────────┴──────────┘
         * 重要逻辑： 查询结果中有几个1，就意味着最大完成步骤是几！！！
         */
        int maxStep = 0;
        while(resultSet.next()){   // 返回结果最多就1行，这个while就走一次!!!
            // 对一行结果中的1进行累加，累加结果即完成的最大步骤数
            for(int i =2;i<totalStep+2;i++) {
                maxStep += resultSet.getInt(i);
            }
        }
        long e = System.currentTimeMillis();

        // 将结果塞回规则参数
        ruleParam.setUserActionSequenceQueriedMaxStep(ruleParam.getUserActionSequenceQueriedMaxStep()+maxStep);

        log.info("clickhouse序列查询，耗时:{},查询到的最大步数:{},条件总步数:{}",(e-s),maxStep,totalStep);
        return maxStep==totalStep;
    }
}

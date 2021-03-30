package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.utils.ConnectionUtils;
import org.apache.flink.api.common.state.ListState;

import java.sql.Connection;
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
public class UserActionSequenceQueryServiceClickhouseImpl implements UserActionSequenceQueryService{

    private Connection conn;

    public UserActionSequenceQueryServiceClickhouseImpl() throws Exception {
        conn = ConnectionUtils.getClickhouseConnection();
    }

    @Override
    public boolean queryActionSequence(String deviceId,ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {


        // 获取规则中，路径模式的总步骤数
        int totalStep = ruleParam.getUserActionSequenceParams().size();

        // 取出查询sql
        String sql = ruleParam.getActionSequenceQuerySql();
        Statement stmt = conn.createStatement();
        // 执行查询
        long s = System.currentTimeMillis();
        ResultSet resultSet = stmt.executeQuery(sql);


        // 从返回结果中进行条件判断
        /**
         * ┌─deviceId─┬─isMatch3─┬─isMatch2─┬─isMatch1─┐
         * │ 000001   │       0  │        0 │        1 │
         * └──────────┴──────────┴──────────┴──────────┘
         * 重要逻辑： 查询结果中有几个1，就意味着最大完成步骤是几！！！
         */
        int maxStep = 0;
        while(resultSet.next()){   // 返回结果最多就1行，这个while就走一次!!!

            // 对一行结果中的1进行累加
            for(int i =2;i<totalStep+2;i++) {
                maxStep += resultSet.getInt(i);
            }

        }
        long e = System.currentTimeMillis();

        // 将结果塞回规则参数
        ruleParam.setUserActionSequenceQueriedMaxStep(maxStep);

        System.out.println("查询了clickhouse,耗时：" + (e-s) +" ms,查询到的最大匹配步骤为：" + maxStep +",条件总步骤数为： " + totalStep);

        return maxStep==totalStep;
    }
}

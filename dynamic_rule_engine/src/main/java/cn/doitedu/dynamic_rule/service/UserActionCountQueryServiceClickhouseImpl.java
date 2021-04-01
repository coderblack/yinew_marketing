package cn.doitedu.dynamic_rule.service;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleAtomicParam;
import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.utils.ClickhouseCountQuerySqlUtil;
import cn.doitedu.dynamic_rule.utils.ConnectionUtils;
import org.apache.flink.api.common.state.ListState;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class UserActionCountQueryServiceClickhouseImpl implements UserActionCountQueryService {


    private Connection conn;

    public UserActionCountQueryServiceClickhouseImpl() throws Exception {
        // 获取clickhouse的jdbc连接对象
        conn = ConnectionUtils.getClickhouseConnection();
    }


    /**
     * 根据给定的deviceId，查询这个人是否满足ruleParam中的所有“次数类"规则条件
     *
     * @param deviceId   要查询的用户
     * @param eventState flink中存储明细事件的state，本实现类中不需要
     * @param ruleParam  规则参数对象
     * @return 条件查询的结果是否成立
     * @throws Exception
     */
    @Override
    public boolean queryActionCounts(String deviceId, ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {

        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        // 遍历每一个原子条件进行查询判断
        for (RuleAtomicParam atomicParam : userActionCountParams) {

            queryActionCounts(deviceId,eventState,atomicParam);

        }

        // 如果走到这一句代码，说明上面的每一个原子条件查询后都满足规则，那么返回最终结果true
        return true;
    }

    /**
     * TODO
     *
     * @param deviceId
     * @param eventState
     * @param atomicParam
     * @return
     * @throws Exception
     */
    @Override
    public boolean queryActionCounts(String deviceId, ListState<LogBean> eventState, RuleAtomicParam atomicParam) throws Exception {
        // 对当前的原子条件拼接查询sql
        String sql = atomicParam.getCountQuerySql();
        // TODO 需要将sql中的deviceId占位符替换成真实deviceId
        System.out.println(sql);

        // 获取一个clickhouse 的jdbc连接
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        /* *
         * deviceId,cnt
         *  000001 ,6
         **/
        // resultSet只有一行，所以while只循环一次
        while (resultSet.next()) {
            // 从结果中取出cnt字段
            int realCnt = (int) resultSet.getLong(2);
            // 将查询结果赛回规则参数对象
            atomicParam.setRealCnts(atomicParam.getRealCnts() + realCnt);
        }

        // 只要有一个原子条件查询结果不满足，则直接返回最终结果false
        if (atomicParam.getRealCnts() < atomicParam.getCnts()) {
            return false;
        } else {
            return true;
        }
    }
}

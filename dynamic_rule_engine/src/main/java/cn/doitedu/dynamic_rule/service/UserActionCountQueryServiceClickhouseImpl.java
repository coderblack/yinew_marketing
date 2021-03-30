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

public class UserActionCountQueryServiceClickhouseImpl implements UserActionCountQueryService{


    Connection conn;
    public UserActionCountQueryServiceClickhouseImpl() throws Exception {
        conn = ConnectionUtils.getClickhouseConnection();
    }


    @Override
    public boolean queryActionCounts(String deviceId, ListState<LogBean> eventState,RuleParam ruleParam) throws Exception {

        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        // 遍历每一个原子条件
        for (RuleAtomicParam atomicParam : userActionCountParams) {

            // 对当前的原子条件拼接查询sql
            String sql = ClickhouseCountQuerySqlUtil.getSql(deviceId, atomicParam);
            System.out.println(sql);

            // 获取一个clickhouse 的jdbc连接
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            // deviceId,cnt
            // 000001  ,6
            while(resultSet.next()){
                int realCnt = (int) resultSet.getLong(2);
                atomicParam.setRealCnts(realCnt);
            }

            if(atomicParam.getRealCnts()<atomicParam.getCnts()) return false;
        }

        // 如果达到这一句话，说明上面的每一个原子条件查询后都满足规则，那么返回最终结果true
        return true;
    }
}
